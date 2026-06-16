# devcloud S3 UI Mock Guidelines

- Treat this mock as a design reference for `docs/design-s3-ui.md`; production implementation should stay in the Rust-served dashboard unless the architecture decision changes.
- Preserve the Object Explorer structure: bucket rail, prefix/object table, inspector, activity drawer, and focused dialogs.
- Keep mock data realistic but non-sensitive. Do not use names that imply real secrets, credentials, or private customer data.
- Prefer dense, calm operational UI over landing-page or marketing composition.
- Keep responsive behavior usable on narrow screens; collapse supporting panels before hiding primary object navigation.
