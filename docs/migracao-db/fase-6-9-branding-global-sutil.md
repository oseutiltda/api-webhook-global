# Fase 6.9 - Plano de Branding Visual Global (sutil e sobrio)

## Objetivo

Aplicar a linguagem visual da Global Cargo em todo o frontend deste projeto sem descaracterizar o produto operacional.

Diretriz central:

- manter o sistema com cara de dashboard profissional;
- usar a marca Global como calibracao de cor e atmosfera, nao como referencia de marketing exuberante;
- evitar contrastes exagerados, gradientes chamativos e blocos muito saturados;
- preservar legibilidade, densidade operacional e estabilidade visual.

## Estado

- Status geral: `concluido`
- Data de criacao: `2026-03-12`
- Responsavel: `Codex`
- Referencia visual principal: `https://globalcargo.com.br/`
- Escopo inicial: `frontend/src`
- Fora de escopo desta fase:
  - mudancas de regra de negocio
  - reestruturacao de navegacao
  - troca ampla de componentes sem necessidade
  - rebranding de backend/worker alem de textos e documentacao visual

## Intencao de design

### Quem e o usuario

Usuario operacional e tecnico que precisa monitorar eventos, webhooks, filas, status e excecoes com leitura rapida e baixa fadiga visual.

### O que ele precisa fazer

- identificar status com clareza;
- localizar falhas e filas rapidamente;
- navegar entre dashboard, worker e login sem ruptura visual;
- confiar que a interface e estavel, corporativa e objetiva.

### Como isso deve parecer

- sobrio
- confiavel
- tecnico
- corporativo
- moderno sem parecer promocional

## Exploracao do dominio

### Conceitos do dominio

- transporte multimodal
- confianca operacional
- rastreabilidade
- pontualidade
- cadeia logistica
- cobertura nacional
- fluxo continuo

### Mundo de cores observado no site Global Cargo

Com base na avaliacao visual e em estilos computados do site oficial:

- azul institucional profundo: `#1E2F5B`
- vermelho operacional/acao: `#EE3124`
- branco limpo: `#FFFFFF`
- cinza neutro de texto: proximo de `#333333`
- cinzas claros de superficie para respiracao visual

### Assinatura visual proposta para este produto

Uso de azul institucional como ancora estrutural e vermelho apenas como sinal de acao/alerta relevante, com superficies claras e bordas discretas.

Isso substitui a tendencia atual de usar varias cores concorrentes e aproxima o sistema de uma identidade mais coesa.

### Defaults a evitar

- dashboard generico com muitos cards coloridos sem hierarquia
- uso excessivo de azul, verde, roxo, laranja e vermelho sem regra comum
- badges e estados com saturacao alta em toda a interface

### O que entra no lugar

- azul Global para foco, destaque estrutural e navegacao
- vermelho Global apenas para CTA primario, alertas importantes e pontos de atencao
- neutros mais consistentes para fundos, cards, bordas e tabelas
- semantica de estados preservada, mas com calibracao mais elegante e menos gritante

## Direcao visual recomendada

### Paleta base

- `--brand-primary`: `#1E2F5B`
- `--brand-primary-soft`: `#2B437A`
- `--brand-primary-faint`: `#EAF0FB`
- `--brand-accent`: `#EE3124`
- `--brand-accent-soft`: `#FDEAE8`
- `--brand-ink`: `#1F2937`
- `--brand-muted`: `#667085`
- `--brand-surface`: `#FFFFFF`
- `--brand-surface-subtle`: `#F7F9FC`
- `--brand-border`: `#D9E1EC`

### Regras de uso

- azul institucional:
  - botoes secundarios
  - titulos-chave
  - links ativos
  - foco e selecao
  - areas de contexto operacional
- vermelho operacional:
  - CTA principal
  - alertas de alta prioridade
  - estados criticos
  - nunca como fundo dominante da pagina
- neutros:
  - canvas principal
  - cards
  - tabelas
  - textos secundarios
  - separadores

### Profundidade e superficies

- fundo principal quase branco, com leve temperatura fria
- cards com contraste baixo e borda suave
- sombras curtas e discretas
- header e paineis com vidro leve apenas quando ja existir no layout
- inputs levemente mais fechados que o fundo para reforcar area interativa

### Tipografia

Para manter sobriedade e reduzir risco de regressao visual:

- manter fonte atual do sistema na primeira etapa
- avaliar `Rubik` apenas como opcao futura para headings, se houver ganho real e sem impacto exagerado

Decisao inicial recomendada:

- nao trocar tipografia global nesta fase de plano

## Mapa de impacto no projeto

### Camada 1 - Tokens e base global

- `frontend/src/app/globals.css`
- `frontend/src/app/layout.tsx`

### Camada 2 - Componentes reutilizaveis

- `frontend/src/components/ui/button.tsx`
- `frontend/src/components/ui/card.tsx`
- `frontend/src/components/ui/badge.tsx`
- `frontend/src/components/ui/table.tsx`
- `frontend/src/components/ui/tabs.tsx`
- `frontend/src/components/ui/chart.tsx`
- `frontend/src/components/ui/separator.tsx`
- `frontend/src/components/ui/input.tsx` se existir no projeto

### Camada 3 - Telas principais

- `frontend/src/components/LoginForm.tsx`
- `frontend/src/app/dashboard/page.tsx`
- `frontend/src/app/page.tsx`
- `frontend/src/app/worker/page.tsx`
- `frontend/src/app/worker1/page.tsx`

### Camada 4 - Assets e consistencia visual

- `frontend/public/logo-global-cima.png`
- `frontend/public/favicon.png`
- outros assets de logo e capa remanescentes no `frontend/public`

## Plano de execucao

### Fase 1 - Auditoria visual e consolidacao de tokens

Objetivo:

- remover variacao cromatica sem regra;
- criar base central para o branding Global sutil.

Entregas:

- mapear classes hardcoded de cor nas paginas principais;
- definir tokens Global no `globals.css`;
- alinhar `primary`, `ring`, `border`, `muted`, `card`, `accent`;
- documentar regra de uso por token.

Critério de aceite:

- nenhuma tela principal depende de azul/roxo/verde arbitrario como identidade primaria;
- o tema global passa a explicar a maior parte da aparencia do app.

### Fase 2 - Ajuste dos componentes compartilhados

Objetivo:

- fazer a linguagem Global emergir pelos componentes, nao por overrides pontuais.

Entregas:

- recalibrar `Button`, `Badge`, `Card`, `Table`, `Tabs` e componentes de grafico;
- reduzir saturacao e contraste agressivo de estados;
- revisar hover, focus e selected para ficarem consistentes com o azul Global.

Critério de aceite:

- componentes compartilhados exibem identidade Global mesmo fora das paginas customizadas;
- foco, hover e active possuem comportamento uniforme.

### Fase 3 - Dashboard principal

Objetivo:

- tornar a tela principal coerente com a marca sem perder densidade operacional.

Entregas:

- suavizar cards de status e metricas;
- aplicar azul institucional em cabecalhos, destaques estruturais e graficos;
- restringir vermelho a CTA, risco e falha relevante;
- revisar header para logo, titulo e microcopys consistentes.

Critério de aceite:

- dashboard parece corporativo e tecnico;
- metricas continuam escaneaveis;
- identidade visual da Global aparece sem poluir.

### Fase 4 - Paginas operacionais do worker

Objetivo:

- uniformizar `worker/page.tsx` e `worker1/page.tsx` com o mesmo sistema visual.

Entregas:

- harmonizar filtros, tabelas, cards, paginação e indicadores;
- ajustar cores de status para linguagem comum;
- revisar hierarquia de tipografia e espacos.

Critério de aceite:

- navegacao entre dashboard e worker nao parece trocar de produto;
- leitura de tabela e status continua forte.

### Fase 5 - Login e detalhes de acabamento

Objetivo:

- consolidar a identidade ja iniciada na tela de login e fechar inconsistencias.

Entregas:

- revisar intensidade do vermelho no CTA;
- alinhar tons do azul do card, focus ring e textos auxiliares;
- conferir favicon, logos e assets remanescentes;
- revisar estados vazios e mensagens auxiliares.

Critério de aceite:

- login fica visualmente conectado ao restante do sistema;
- nenhum asset antigo de BMX fica aparente em fluxos comuns.

### Fase 6 - Validacao visual e fechamento

Objetivo:

- validar que a mudanca e sutil, estavel e consistente.

Entregas:

- revisar telas em desktop e mobile com Playwright;
- registrar capturas de referencia antes/depois quando necessario;
- executar `eslint`, `typecheck` e `prettier`;
- atualizar tracking mestre e checkpoint seguinte.

Critério de aceite:

- sem regressao visual gritante;
- sem quebra de layout;
- sem aumento indevido de ruído cromatico;
- documentacao pronta para continuidade.

## Checklist operacional

- [ ] Levantar mapa completo de cores hardcoded no frontend
- [ ] Definir tokens `Global` em `globals.css`
- [ ] Ajustar tema base (`primary`, `accent`, `ring`, `border`, `muted`, `card`)
- [ ] Documentar regra de uso das cores no proprio plano
- [ ] Revisar `Button`
- [ ] Revisar `Badge`
- [ ] Revisar `Card`
- [ ] Revisar `Table`
- [ ] Revisar `Tabs`
- [ ] Revisar componentes de grafico
- [ ] Harmonizar `dashboard/page.tsx`
- [ ] Harmonizar `page.tsx`
- [ ] Harmonizar `worker/page.tsx`
- [ ] Harmonizar `worker1/page.tsx`
- [ ] Revisar `LoginForm.tsx`
- [ ] Revisar assets de logo/favicon residuais
- [ ] Validar desktop via Playwright
- [ ] Validar mobile via Playwright
- [ ] Executar `eslint`
- [ ] Executar `typecheck`
- [ ] Executar `prettier --check`
- [ ] Atualizar `docs/migracao-db/progress-tracking.md`

## Checklist de sobriedade

- [ ] Azul Global usado como ancora estrutural, nao como tinta em excesso
- [ ] Vermelho Global usado apenas em CTA e alertas de maior prioridade
- [ ] Fundos continuam claros e discretos
- [ ] Cards nao ficam promocionais nem "marketingizados"
- [ ] Tabelas e grids preservam legibilidade
- [ ] Estados semanticos continuam claros mesmo com menor saturacao
- [ ] A interface nao parece outro produto

## Riscos e mitigacoes

### Risco 1 - Saturacao excessiva

Mitigacao:

- validar cada tela contra o criterio de sobriedade;
- limitar vermelho a papeis pequenos e claros.

### Risco 2 - Inconsistencia entre telas

Mitigacao:

- atacar tokens e componentes antes das paginas;
- evitar correcoes ad hoc isoladas.

### Risco 3 - Regressao em legibilidade operacional

Mitigacao:

- preservar contraste de texto e tabelas;
- revisar badges e estados com dados reais ou mocks existentes.

### Risco 4 - Mistura de branding com refactor funcional

Mitigacao:

- manter escopo estritamente visual;
- nao alterar fluxos de dados, consultas ou regras de negocio.

## Evidencias esperadas por etapa

- lista de arquivos alterados
- screenshots ou validacao Playwright quando aplicavel
- saida de `eslint`
- saida de `typecheck`
- saida de `prettier --check`
- observacoes sobre residuos de branding antigo

## Tracking de progresso

### Status por fase

| Fase | Descricao | Status | Evidencia | Proximo passo |
| --- | --- | --- | --- | --- |
| 1 | Auditoria visual e tokens Global | feito | Tokens, superfícies e estados centralizados em `globals.css` | Consolidar historico no tracking mestre |
| 2 | Componentes compartilhados | feito | `Button`, `Badge`, `Card`, `Input`, `Table`, `Tabs`, `Chart`, `ScrollArea` refinados | Manter consistencia em novas telas |
| 3 | Dashboard principal | feito | `dashboard/page.tsx` e `page.tsx` recalibrados para a linguagem Global | Monitorar ajustes finos com uso real |
| 4 | Paginas do worker | feito | `worker/page.tsx` e `worker1/page.tsx` harmonizados | Monitorar densidade de leitura nas tabelas |
| 5 | Login e acabamento | feito | `LoginForm.tsx`, favicon, metadados e logo Global aplicados | Revisar novos assets somente se surgirem |
| 6 | Validacao final | feito | Playwright desktop/mobile + `eslint` + `typecheck` + `prettier` executados | Enderecar warnings preexistentes em ciclo separado |

### Registro incremental

| Data | Checkpoint | Alteracao | Status | Evidencia | Proximo passo |
| --- | --- | --- | --- | --- | --- |
| 2026-03-12 | F6.9-plan-branding-global-sutil | Plano visual criado com referencia no site oficial da Global Cargo e foco em aplicacao sutil no frontend | feito | Documento criado e alinhado ao tracking mestre | Iniciar Fase 1 com auditoria de tokens e cores hardcoded |
| 2026-03-12 | F6.9.1-execucao-branding-global-sutil | Branding sutil Global aplicado em tokens, componentes, login, dashboard e telas operacionais | feito | Validacao local por Playwright + `typecheck` + `prettier` + `eslint` sem erros | Manter apenas refinamentos pontuais futuros |

## Criterio de pronto desta fase

Esta fase sera considerada pronta quando:

- o frontend inteiro compartilhar o mesmo sistema de cores Global;
- a interface continuar sobria e profissional;
- dashboards e telas operacionais parecerem parte do mesmo produto;
- nao houver residuos visuais evidentes de branding BMX nos fluxos principais;
- o tracking estiver atualizado com evidencias de validacao.

## Proximo checkpoint

`F6.9-fechamento-branding-global-sutil`
