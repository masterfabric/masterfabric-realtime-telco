# Contributing

Thanks for your interest. This is a lab repository: changes are welcome when they keep it runnable on a
laptop and keep every number in the README reproducible.

## Ground rules

- **Measured numbers only.** If a change affects results, re-run the affected step (`make live`,
  `make evaluate`, `make bench`, `make maintain`) and commit the new `docs/results/*.json` together with
  regenerated charts (`make assets`). Never edit a results file by hand.
- **Tune on the development seed, judge on the held-out one.** Detector changes are tuned with
  `make offline SEED=7` and reported from a live run on seed 2026.
- **One logical change per commit**, with a Conventional Commit subject (`feat:`, `fix:`, `docs:`,
  `perf:`, `test:`, `chore:`) and a body that explains why.
- **No secrets.** `.env` is generated from `.env.example` and is gitignored. Lab credentials only.
- Tests: `make test-unit` must pass. Add a test for every detector rule you change.

## Development loop

```bash
make up && make test-unit
make offline SEED=7          # fast: pandas replay of the same detector
make live && make evaluate   # full: about 13 minutes
```

## License of contributions

By contributing you agree that your contribution is licensed under the AGPL-3.0, as described in
[LICENSE](LICENSE) and [NOTICE](NOTICE).
