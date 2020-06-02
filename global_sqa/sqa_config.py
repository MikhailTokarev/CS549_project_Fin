### sqa_config.py ###

# Scheme: "postgres+psycopg2://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>"

DATABASE_URI = 'postgres+psycopg2://{username}:{password}@{ip_addr}:{port}/{db_name}'.format(
    username = 'postgres',
    password = 'password',
    ip_addr  = 'localhost',
    port     = '5432',
    db_name  = 'movies'
)

