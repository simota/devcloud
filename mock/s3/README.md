# devcloud S3 UI Mock

This is the interactive design mock for the devcloud S3 dashboard.

The mock visualizes the `Object Explorer` direction described in:

- `docs/design-s3-ui.md`
- `docs/design-s3-compat.md`

## Scope

This mock is a reference artifact only. The production dashboard should remain a Go-served static HTML/CSS/JS implementation without a React runtime dependency unless that architectural decision is explicitly changed later.

The mock covers:

- bucket sidebar
- prefix/object browser
- object inspector
- metadata, versions, multipart, and preview tabs
- create bucket and delete confirmation flows
- desktop, tablet, and mobile layouts

## Running

```bash
npm install
npm run dev
```

Then open the Vite URL printed by the dev server.

## Notes

- Mock data lives in `src/app/components/s3/mockData.ts`.
- The UI uses fixture data and does not call the real devcloud S3 API.
- Do not port the mock dependency graph into `internal/dashboard`.
