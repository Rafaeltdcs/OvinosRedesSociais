import db
import coleta_dados as cd

# Cria conexão no banco de dados
db.db_conection_eng() #Cria a conexão com o banco
db.db_conection_session() #Cria a session

# Coletar o dados
coletor = cd.Coletor() # Inicia a classe
df = coletor.get_db_id() #Pega o id da tabela dimensão
views = coletor.get_views(df)

# Escrever dados no banco
db.wr_views_youtube(views)
