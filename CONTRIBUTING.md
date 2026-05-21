# Contributing to InstaNode worker

## Filing issues

Bugs and feature requests: https://github.com/InstaNode-dev/worker/issues.

For platform-wide issues (provisioning, billing, deploys) file at the [api repo](https://github.com/InstaNode-dev/api/issues) instead.

## Workflow

```
git clone https://github.com/InstaNode-dev/worker
cd worker
go build ./...
go vet ./...
go test ./... -short -p 1
```

(For infra: substitute YAML lint / kubeconform / shellcheck per the validate workflow.)

All gates must be green before opening a PR.

## Style

- Follow existing patterns in the file you're touching.
- Tests next to source.
- Public symbols get godoc comments.
- Errors wrapped with `fmt.Errorf("context: %w", err)`.

## PR checklist

- Local gate green
- New behavior → test
- New public symbol → godoc
- Commit message: short imperative subject, fuller body explaining the why
- Include the `Co-Authored-By` trailer for AI-assisted commits

## License

MIT. By contributing, you agree your contributions are licensed under the same.
