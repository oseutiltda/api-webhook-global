type HttpMethod = 'get' | 'post' | 'put' | 'patch' | 'delete';

type EndpointDoc = {
  method: HttpMethod;
  path: string;
  summary: string;
  tag: string;
  requiresTokenQuery?: boolean;
  requestBodyExample?: Record<string, unknown>;
};

const endpoints: EndpointDoc[] = [
  {
    method: 'post',
    path: '/api/CTe/InserirCte',
    summary: 'Recebe e grava CT-e em staging (ctes)',
    tag: 'CTe',
    requiresTokenQuery: true,
    requestBodyExample: {
      id: 900001,
      authorization_number: 123456789,
      status: 'authorized',
      xml: '<cteProc>...</cteProc>',
      event_xml: null,
    },
  },
  {
    method: 'post',
    path: '/api/NFSe/InserirNFSe',
    summary: 'Recebe e grava NFSe',
    tag: 'NFSe',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/api/CIOT/InserirContasPagarCIOT',
    summary: 'Recebe CIOT de contas a pagar',
    tag: 'CIOT',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/api/CIOT/CancelarContasPagarCIOT',
    summary: 'Recebe cancelamento de CIOT',
    tag: 'CIOT',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/api/Pessoa/InserirPessoa',
    summary: 'Recebe e grava Pessoa',
    tag: 'Pessoa',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/api/ContasPagar/InserirContasPagar',
    summary: 'Recebe e grava Contas a Pagar',
    tag: 'ContasPagar',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/api/ContasReceber/InserirContasReceber',
    summary: 'Recebe e grava Contas a Receber',
    tag: 'ContasReceber',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/api/ContasReceber/InserirContasReceberBaixa',
    summary: 'Recebe e grava baixa de Contas a Receber',
    tag: 'ContasReceber',
    requiresTokenQuery: true,
  },
  {
    method: 'post',
    path: '/webhooks/cte/autorizado',
    summary: 'Webhook de CT-e autorizado',
    tag: 'Webhooks',
  },
  {
    method: 'post',
    path: '/webhooks/cte/cancelado',
    summary: 'Webhook de CT-e cancelado',
    tag: 'Webhooks',
  },
  {
    method: 'get',
    path: '/health',
    summary: 'Healthcheck simples',
    tag: 'Infra',
  },
  {
    method: 'get',
    path: '/api/health',
    summary: 'Healthcheck detalhado',
    tag: 'Infra',
  },
];

const withLeadingSlash = (value: string): string => {
  return value.startsWith('/') ? value : `/${value}`;
};

const buildPaths = () => {
  const paths: Record<string, Record<string, unknown>> = {};

  endpoints.forEach((endpoint) => {
    const path = withLeadingSlash(endpoint.path);
    if (!paths[path]) paths[path] = {};

    const parameters = endpoint.requiresTokenQuery
      ? [
          {
            in: 'query',
            name: 'token',
            required: true,
            schema: { type: 'string' },
            description: 'Token fixo de autenticação por query string',
          },
        ]
      : [];

    const requestBody =
      endpoint.method === 'post' || endpoint.method === 'put' || endpoint.method === 'patch'
        ? {
            required: true,
            content: {
              'application/json': {
                schema: { type: 'object', additionalProperties: true },
                ...(endpoint.requestBodyExample ? { example: endpoint.requestBodyExample } : {}),
              },
            },
          }
        : undefined;

    paths[path][endpoint.method] = {
      tags: [endpoint.tag],
      summary: endpoint.summary,
      parameters,
      ...(requestBody ? { requestBody } : {}),
      responses: {
        200: { description: 'Sucesso' },
        201: { description: 'Criado com sucesso' },
        400: { description: 'Requisição inválida' },
        401: { description: 'Não autorizado' },
        500: { description: 'Erro interno' },
      },
    };
  });

  return paths;
};

export const openApiSpec = {
  openapi: '3.0.3',
  info: {
    title: 'API Webhook Global',
    version: '1.0.0',
    description: 'Documentação operacional da API de recebimento de webhooks e integração local.',
  },
  servers: [{ url: '/', description: 'Servidor atual' }],
  tags: [
    { name: 'CTe' },
    { name: 'NFSe' },
    { name: 'CIOT' },
    { name: 'Pessoa' },
    { name: 'ContasPagar' },
    { name: 'ContasReceber' },
    { name: 'Webhooks' },
    { name: 'Infra' },
  ],
  paths: buildPaths(),
};

export const renderSwaggerUiHtml = (specUrl = '/docs.json'): string => {
  return `<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: '${specUrl}',
        dom_id: '#swagger-ui',
        deepLinking: true,
      });
    </script>
  </body>
</html>`;
};
