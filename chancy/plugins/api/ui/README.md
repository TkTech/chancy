# chancy-ui

This directory contains a single-page application (SPA) that uses the Chancy
API to display a simple dashboard of the current state of the Chancy cluster.

The default API plugin will serve the built version of this project when you
navigate to the root `/` out of the `chancy.plugins.api.dist` directory.

## Development

Install the dependencies by running the following command:

```bash
npm install
```

To start the development server, run the following command:

```bash
npm run dev
```

This will start a development server that will automatically reload when you
make changes to the source code on `http://localhost:5134`.

## Building

To build the project, run the following command:

```bash
npm run build
```

Do not commit the `../dist` directory to the repository. It is automatically
generated when the project is built for release to ensure it's up-to-date.
