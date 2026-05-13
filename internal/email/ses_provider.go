package email

// ses_provider.go — Amazon SES v2 transactional email provider.
//
// Implements EmailProvider against the sesv2.SendEmail API:
//
//   POST /v2/email/outbound-emails (region-specific endpoint, signed with SigV4)
//   Content.Template.{TemplateName, TemplateData=json(evt.Params)}
//
// Result classification — these line up with SendClass so the forwarder
// doesn't need to know SES's error model:
//
//   nil err                                                    → nil                                            (success — forwarder advances cursor)
//   MessageRejected / BadRequest / NotFound / Unrecognized…    → *SendError{Class: SendClassPermanent}          (forwarder advances + logs ERROR)
//   Throttling / InternalServiceError / SendingPaused / net    → *SendError{Class: SendClassTransient}          (forwarder holds cursor)
//   no template configured for kind                            → *SendError{Class: SendClassSkippedNoTemplate}  (forwarder advances silently)
//
// The "4xx-advances" / "client-fault advances" rule is identical to the
// Brevo provider: a single audit row with malformed content shouldn't
// pin the queue. We log loudly so the poisoned row is visible.
//
// SES sandbox caveat — fresh AWS accounts are in the SES sandbox where
// every recipient address must be verified. Operators flipping
// EMAIL_PROVIDER=ses on a sandbox account will see MessageRejected on
// every unverified recipient (Permanent → cursor advances, row logged).
// Production-readiness requires a "SES production access" request to AWS.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"
)

// SESConfig is the SES-specific configuration plucked from env at
// startup. AWSAccessKey/AWSSecretKey come from SES_AWS_ACCESS_KEY_ID /
// SES_AWS_SECRET_ACCESS_KEY; FromEmail must be a verified SES identity;
// TemplateNames is a JSON object mapping audit_log.kind → SES template
// name (SES references templates by name, not by id — unlike Brevo).
//
// Config-not-code mapping so a marketing operator can wire a new event
// in the SES console + one line in SES_TEMPLATE_NAMES without a worker
// release. Kinds not in the map produce SendClassSkippedNoTemplate.
type SESConfig struct {
	AWSRegion     string            // SES_AWS_REGION
	AWSAccessKey  string            // SES_AWS_ACCESS_KEY_ID
	AWSSecretKey  string            // SES_AWS_SECRET_ACCESS_KEY
	FromEmail     string            // SES_FROM_EMAIL (must be a verified SES identity)
	TemplateNames map[string]string // SES_TEMPLATE_NAMES: kind → SES template name
}

// sesSendEmailAPI is the minimal surface of *sesv2.Client we depend on.
// Lets tests swap in a fake without spinning up an SES mock server. The
// real *sesv2.Client satisfies this interface.
type sesSendEmailAPI interface {
	SendEmail(ctx context.Context, params *sesv2.SendEmailInput, optFns ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error)
}

// SESProvider is the live SES implementation. Constructed once at boot
// via NewSESProvider and reused across every forwarder tick. The SES
// client is goroutine-safe; templates is read-only after construction.
type SESProvider struct {
	client    sesSendEmailAPI
	fromEmail string
	templates map[string]string
}

// NewSESProvider validates SESConfig, builds an aws.Config + sesv2.Client,
// and returns the live provider. Fails fast on:
//   - missing AWSRegion (SES is regional — no sensible default)
//   - missing access key or secret (operator opted into SES, silent noop would hide misconfig)
//   - missing FromEmail (SES requires a verified sender address)
//
// Empty TemplateNames is NOT fatal — every send returns SkippedNoTemplate
// so operators can bring up SES with credentials first and add templates
// incrementally (mirrors the Brevo provider's behaviour).
func NewSESProvider(cfg SESConfig) (*SESProvider, error) {
	if cfg.AWSRegion == "" {
		return nil, fmt.Errorf("ses: SES_AWS_REGION required when EMAIL_PROVIDER=ses (SES is regional, no default)")
	}
	if cfg.AWSAccessKey == "" || cfg.AWSSecretKey == "" {
		return nil, fmt.Errorf("ses: SES_AWS_ACCESS_KEY_ID and SES_AWS_SECRET_ACCESS_KEY required when EMAIL_PROVIDER=ses (leave EMAIL_PROVIDER unset to disable email)")
	}
	if cfg.FromEmail == "" {
		return nil, fmt.Errorf("ses: SES_FROM_EMAIL required (must be a verified SES identity)")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKey, cfg.AWSSecretKey, ""),
		),
	)
	if err != nil {
		// The only reason LoadDefaultConfig fails with explicit creds + region
		// is a malformed shared-config file in the environment. That's a
		// programmer/operator error → fail fast at boot, not silently.
		return nil, fmt.Errorf("ses: aws config load failed: %w", err)
	}

	tmpls := cfg.TemplateNames
	if tmpls == nil {
		tmpls = map[string]string{}
	}

	return &SESProvider{
		client:    sesv2.NewFromConfig(awsCfg),
		fromEmail: cfg.FromEmail,
		templates: tmpls,
	}, nil
}

// Name returns the stable provider identifier used in slog/metric labels.
func (p *SESProvider) Name() string { return providerNameSES }

// SendEvent implements EmailProvider.SendEvent. Maps EventEmail.Kind to
// an SES template name via p.templates, marshals evt.Params to JSON
// (SES's TemplateData wire format), calls SendEmail, and classifies the
// error per the table at the top of this file.
func (p *SESProvider) SendEvent(ctx context.Context, evt EventEmail) error {
	tmplName, ok := p.templates[evt.Kind]
	if !ok {
		// Operator hasn't mapped this kind to an SES template yet —
		// forwarder advances silently. No API call avoids burning
		// SES quota on rows that wouldn't have rendered anything.
		return &SendError{
			Class:   SendClassSkippedNoTemplate,
			Message: fmt.Sprintf("ses: no template configured for kind %q", evt.Kind),
		}
	}
	if evt.Recipient == "" {
		// Defensive — forwarder filters orphan rows but a future caller path
		// might not. Permanent: this row will never sprout an email later.
		return &SendError{
			Class:   SendClassPermanent,
			Message: "ses: empty recipient",
		}
	}

	// SES expects TemplateData as a JSON string keyed by template
	// variables — flat string→string matches the EventEmail.Params
	// contract exactly, no per-event shape negotiation.
	paramsJSON, err := json.Marshal(evt.Params)
	if err != nil {
		// Marshalling map[string]string can only fail for ill-formed
		// keys (never happens) — Permanent so we advance past the row
		// instead of looping forever on a programmer bug.
		slog.Error("email.ses.marshal_failed",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"error", err,
		)
		return &SendError{Class: SendClassPermanent, Cause: err, Message: "ses: marshal params"}
	}

	input := &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(p.fromEmail),
		Destination: &sestypes.Destination{
			ToAddresses: []string{evt.Recipient},
		},
		Content: &sestypes.EmailContent{
			Template: &sestypes.Template{
				TemplateName: aws.String(tmplName),
				TemplateData: aws.String(string(paramsJSON)),
			},
		},
	}

	_, err = p.client.SendEmail(ctx, input)
	if err != nil {
		return classifySESError(err, evt, tmplName)
	}

	slog.Info("email.ses.event_sent",
		"kind", evt.Kind,
		"recipient", evt.Recipient,
		"template", tmplName,
	)
	return nil
}

// classifySESError maps an SES SDK error into a *SendError with the
// appropriate Class. Three buckets — see the table at the top of this
// file. Logging happens here so each branch produces a structured row
// with the SES error code intact for dashboarding.
//
// Decision tree (in order):
//  1. context.Canceled / context.DeadlineExceeded → Transient
//     (the caller's context died; the forwarder will hold the cursor and
//     retry next tick — the same audit row can fire fresh).
//  2. smithy.APIError → look at ErrorCode() first (specific overrides),
//     then ErrorFault() (server-fault → Transient, client-fault → Permanent).
//  3. net.Error / unwrapped errors → Transient (network blip, dns failure,
//     etc. — definitionally retryable).
//  4. Unknown error type → Transient (fail-safe: hold cursor, surface in logs).
func classifySESError(err error, evt EventEmail, tmplName string) error {
	if err == nil {
		return nil
	}

	// Context cancellation / deadline — Transient. Forwarder retries next tick.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		slog.Warn("email.ses.context_canceled",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"error", err,
		)
		return &SendError{Class: SendClassTransient, Cause: err, Message: "ses: context canceled"}
	}

	// SES typed errors implement smithy.APIError. Inspect ErrorCode() first
	// for the few cases where ErrorFault() alone gives the wrong answer
	// (e.g. SendingPausedException is FaultClient but is retryable once
	// sending is unpaused — classify as Transient so we don't burn through
	// the row when an operator forgets to flip the dashboard switch).
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch code {
		// Explicitly retryable client-faults: code says "client wrong" but
		// the right action is wait + retry, not advance past the row.
		case "ThrottlingException", "TooManyRequestsException",
			"RequestThrottledException", "SlowDown",
			"SendingPausedException":
			slog.Warn("email.ses.transient_throttle",
				"kind", evt.Kind,
				"recipient", evt.Recipient,
				"code", code,
				"message", apiErr.ErrorMessage(),
			)
			return &SendError{
				Class:   SendClassTransient,
				Cause:   err,
				Message: fmt.Sprintf("ses: %s: %s", code, apiErr.ErrorMessage()),
			}

		// Explicitly permanent: payload is bad, auth is wrong, template
		// doesn't exist, account is suspended. Advance cursor + log ERROR.
		case "MessageRejected", "BadRequestException", "InvalidParameterException",
			"InvalidParameterValueException", "NotFoundException",
			"MailFromDomainNotVerifiedException", "AccountSuspendedException",
			"UnrecognizedClientException", "InvalidClientTokenId",
			"SignatureDoesNotMatch", "AccessDeniedException":
			slog.Error("email.ses.permanent",
				"kind", evt.Kind,
				"recipient", evt.Recipient,
				"code", code,
				"template", tmplName,
				"message", apiErr.ErrorMessage(),
			)
			return &SendError{
				Class:   SendClassPermanent,
				Cause:   err,
				Message: fmt.Sprintf("ses: %s: %s", code, apiErr.ErrorMessage()),
			}

		// 5xx-equivalent: SES is unhealthy on their side.
		case "InternalServiceErrorException", "ServiceUnavailableException":
			slog.Warn("email.ses.transient_5xx",
				"kind", evt.Kind,
				"recipient", evt.Recipient,
				"code", code,
				"message", apiErr.ErrorMessage(),
			)
			return &SendError{
				Class:   SendClassTransient,
				Cause:   err,
				Message: fmt.Sprintf("ses: %s: %s", code, apiErr.ErrorMessage()),
			}
		}

		// Unknown error code — fall back to ErrorFault(): server-fault
		// is Transient, client-fault is Permanent.
		switch apiErr.ErrorFault() {
		case smithy.FaultServer:
			slog.Warn("email.ses.transient_server_fault",
				"kind", evt.Kind,
				"recipient", evt.Recipient,
				"code", code,
				"message", apiErr.ErrorMessage(),
			)
			return &SendError{
				Class:   SendClassTransient,
				Cause:   err,
				Message: fmt.Sprintf("ses: server-fault %s: %s", code, apiErr.ErrorMessage()),
			}
		default:
			// FaultClient or FaultUnknown — treat as Permanent so a bad
			// row can't pin the queue forever.
			slog.Error("email.ses.permanent_client_fault",
				"kind", evt.Kind,
				"recipient", evt.Recipient,
				"code", code,
				"template", tmplName,
				"message", apiErr.ErrorMessage(),
			)
			return &SendError{
				Class:   SendClassPermanent,
				Cause:   err,
				Message: fmt.Sprintf("ses: client-fault %s: %s", code, apiErr.ErrorMessage()),
			}
		}
	}

	// Network error — Transient by definition (dns, connection refused,
	// reset, timeout, etc.).
	var netErr net.Error
	if errors.As(err, &netErr) {
		slog.Warn("email.ses.network",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"error", err,
		)
		return &SendError{Class: SendClassTransient, Cause: err, Message: "ses: network"}
	}

	// Unknown error type — fail-safe Transient so unknown errors hold the
	// cursor (mirrors the package-level ClassOf default).
	slog.Warn("email.ses.unknown_error",
		"kind", evt.Kind,
		"recipient", evt.Recipient,
		"error", err,
	)
	return &SendError{Class: SendClassTransient, Cause: err, Message: "ses: unknown error"}
}

// Compile-time interface satisfaction check — proves the seam fits.
// If a future EmailProvider change breaks SES, the build fails here,
// not silently when an operator flips EMAIL_PROVIDER=ses.
var _ EmailProvider = (*SESProvider)(nil)
