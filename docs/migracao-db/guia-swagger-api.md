# Guia - Swagger da API (`/docs`)

## Enderecos

- UI: `/docs`
- OpenAPI JSON: `/docs.json`

## Como usar

1. Suba o backend (`docker compose up -d --build backend`).
2. Acesse `http://localhost:3000/docs`.
3. Teste os endpoints pela interface.

## Observacoes

- A UI usa `swagger-ui` via CDN (`unpkg`).
- Se a maquina estiver sem internet, use `http://localhost:3000/docs.json` para consumir a especificacao JSON diretamente.
- Os endpoints principais de webhook/API ja estao listados e organizados por tags.
