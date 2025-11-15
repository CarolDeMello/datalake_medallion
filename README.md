# Tecnologias usadas

Primeiramente foi criado um ambiente virtual para encapsular as bibliotecas usadas.
Isso é uma boa prática para garantir que o ambiente do projeto esteja configurado corretamente e contido internamente, caso necessite de replicação futura.

- Bibliotecas Python:
    - requests: para conseguir estabelecer conexão com a fonte
    - zipfile: para extrair o zip e obter o csv
    - pyspark: Para toda a manipulação de dados e transformações das camadas medalhão, usando principalmente Spark SQL

# Camada Bronze

Foi feito a leitura via get da URL da fonte dos dados (usando a biblioteca requests). 
Então foi feita a extração do ZIP para obter o arquivo CSV, o qual ficou hospedado no caminho temporário "dados/landing_zone"

Dando sucesso, foi feita a leitura com Spark do CSV que está na landing zone, e persistiu na camada bronze (path dados/bronze)

*Neste último passo poderia ter sido já feita a gravação em parquet, pois isso resultaria em uma leitura mais otimizada na camada silver, mas segui o enunciado o qual diz para a conversão ser feita apenas na silver. 


# Camanda Silver

Comecei com a leitura do arquivo na camada bronze e criação de uma view temporária (onde os dados ficarão contidos para manipulação via Spark SQL) já filtrando apenas as colunas que serão relevantes para o uso na camada gold.

Primeiro foi assegurada a tipagem das colunas, convertendo o número de beneficiários para int e os demais como string (pois alguns códigos de operadora poderiam se iniciar com 0, e ele se perderia se ficasse como int).

Também foi feito o mascaramento do codigo do plano pois ele pode ser um dado sensível que não deve ser trafegado aberto. Para isso a máscara tem o padrão XXXXX1234

Em seguida foi feita a limpeza dos dados, retirando todos os nulos, vazios e número de beneficiários que fossem menor ou igual a 0, e persistido o resultado no path "dados/silver"

# Camada Gold

Ao iniciar a camada gold, primeiro analisei o que as perguntas propostas tinham em comum para entender se poderia fazer algum pré-calculo e salvar na camada gold.

Assim, foi identificado que o a soma do número de beneficiários será necessária para as três questões, então fiz seu pré-calculo agregando por município, operadora e faixa de idade. Ao invés de na camada analítica todas as linhas serem lidas, só vai ser feita a leitura de uma única que vai possuir a informação agrupada e calculada.

Em seguida fiz um particionamento pelo código da operadora, pois este otimiza a resposta da primeira pergunta, e ele possui uma boa cardinalidade e distribuição para novas consultas futuras, eliminando a busca por todos os dados podendo apenas consultar por operadora, se necessário.

# Perguntas e Respostas

No início fiz a leitura da gold e fiz um caching criando uma view para que as três questões seguintes pudessem pegar em memória os dados necessários, não precisar chamar toda vez a fonte.

# Considerações finais

Para este desafio foi solicitado o uso de SQL ao longo das camadas. Porém, vale ressaltar que dependendo da necessidade, uma alternativa ao uso de Spark SQL seria fazer a manipulação usando Dataframes. Neste caso o SQL é ótimo para legibilidade e manutenção por analistas que não necessariamente usam o PySpark no dia a dia. O Dataframe seria mais ideal caso quisesse fazer criação de UDFs (User Defined Functions) e necessitasse de integrações com outras bibliotecas mais complexas.

Este foi o projeto feito de forma local. Se fosse necessário escalar para produção utilizando o ambiente AWS, usaria o AWS Glue para criação dos scripts em PySpark, e StepFunction para a orquestração deles. O armazenamento poderia ser utilizando S3 e a análise exploratória em Athena, o qual faz buscas diretas no S3, ou usando Redshift se for necessário um uso de BI mais robusto.

