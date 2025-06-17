FLUXO:

ENTITY_BATCH = Entidade de lote de entidades

Arquivo Csv contem diversas linhas com os dados pra criação de uma entity. 

POST entity_batch (recebe filename) -> backend -> bucket s3 -> devolve link pre assinado -> tela faz upload no bucket e da um put de file uploaded.

PUT file uploaded -> backend -> Abre esse arquivo e passa ele por um processo de validação, uma vez validado itera por cada linha do arquivo e posta uma mensagem na fila pra cada uma das entidades do arquivo.

FILA -> Posta a entidade no sistema de cadastro de entidades e atualiza o status da entidade no backend (Essa entidade existe tanto no sistema de cadastro quanto no sistema de arquivos para fin de loggins)

Decisões Arquiteturais:

- Todo processo performance-sensitive acontece via fila, podendo acontecer paralelamente e facilmente escalável aumentando o número / potência dos consumers dessa fila.
- Bucket S3 pra deixar toda a logica de upload de arquivo desacoplada do backend, assim so fazendo leitura (muito mais eficiente)

