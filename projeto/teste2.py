import pandas as pd
import matplotlib.pyplot as plt
import duckdb

# Caminhos para os arquivos
csv_path = "./database/OCUPANET.csv"
db_path = "./database/database_cd.db"

# Conexão com o banco de dados
conn = duckdb.connect(db_path)

# IDs dos municípios e seus nomes
ids_municipios = [431390, 431020, 431410]  
municipios_nome = {431390: 'Panambi', 431020: 'Ijuí', 431410: 'Passo Fundo'}

# Carregar dados de cargos
try:
    cargos = pd.read_csv(csv_path, encoding='ISO-8859-1', on_bad_lines='skip', delimiter=',')  
    cargos.columns = cargos.columns.str.strip().str.replace('"', '')  
    cargos.set_index('ID_OCUPA_N', inplace=True)

except KeyError as e:
    print(f"Erro: {e}. Verifique se a coluna 'ID_OCUPA_N' existe no DataFrame.")
    exit(1)  
except Exception as e:
    print(f"Ocorreu um erro ao ler o arquivo: {e}")
    exit(1)  

def carregar_dados_acidentes(ano):
    query = f"""
    SELECT ID_MUNICIP, ID_OCUPA_N, NU_IDADE_N
    FROM dadosacidentetrabalho
    WHERE ID_MUNICIP IN ({','.join(map(str, ids_municipios))})
    AND NU_ANO = {ano}
    """
    return pd.read_sql_query(query, conn)

# Carregar dados de acidentes
dados_2022 = carregar_dados_acidentes(2022)
dados_2023 = carregar_dados_acidentes(2023)

# Adicionar a coluna de Cargo
dados_2022['Cargo'] = dados_2022['ID_OCUPA_N'].map(cargos['Descricao'])
dados_2023['Cargo'] = dados_2023['ID_OCUPA_N'].map(cargos['Descricao'])

# Adicione a coluna de município
dados_2022['Municipio'] = dados_2022['ID_MUNICIP'].map(municipios_nome)
dados_2023['Municipio'] = dados_2023['ID_MUNICIP'].map(municipios_nome)

# Função para extrair a idade
def extrair_idade(codigo_idade):
    return int(str(codigo_idade)[-2:])

# Função para definir faixa etária
def faixa_etaria(idade):
    if idade < 18:
        return '<18'
    elif 18 <= idade <= 25:
        return '18-25'
    elif 26 <= idade <= 35:
        return '26-35'
    elif 36 <= idade <= 50:
        return '36-50'
    else:
        return '>50'

# Função para processar dados de acidentes por faixa etária e cargo
def processar_dados(dados):
    dados['IDADE_CORRETA'] = dados['NU_IDADE_N'].apply(extrair_idade)
    dados['FAIXA_ETARIA'] = dados['IDADE_CORRETA'].apply(faixa_etaria)
    dados['CIDADE'] = dados['ID_MUNICIP'].map(municipios_nome)
    return dados.groupby(['CIDADE', 'Cargo', 'FAIXA_ETARIA']).size().unstack(fill_value=0)

# Processar dados de faixas etárias para 2022 e 2023
faixas_2022 = processar_dados(dados_2022)
faixas_2023 = processar_dados(dados_2023)

# Gráficos de faixas etárias por cargo
def plotar_faixas_etarias_por_cargo():
    for municipio in municipios_nome.values():
        fig, ax = plt.subplots(figsize=(12, 8))

        # Criar DataFrame para o município específico
        data_2022 = faixas_2022.loc[municipio] if municipio in faixas_2022.index else pd.DataFrame(0, index=faixas_2022.columns)
        data_2023 = faixas_2023.loc[municipio] if municipio in faixas_2023.index else pd.DataFrame(0, index=faixas_2023.columns)

        # Concatenar os dados de 2022 e 2023
        data = pd.concat([data_2022, data_2023], axis=1)
        
        # Renomear as colunas data.columns = ['2022', '2023']

        # Filtrar apenas os 5 cargos com mais acidentes
        top_5 = data.sum(axis=1).nlargest(5).index
        data = data.loc[top_5]

        # Plotar os dados
        data.plot(kind='bar', ax=ax, alpha=0.8)

        # Configurações do gráfico
        ax.set_title(f'Top 5 Faixas Etárias por Cargo - {municipio}', fontsize=16)
        ax.set_ylabel('Quantidade de Acidentes', fontsize=12)
        ax.set_xlabel('Cargo', fontsize=12)
        ax.legend(title='Ano', fontsize=10)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.show()

# Chamar a função para plotar as faixas etárias por cargo
plotar_faixas_etarias_por_cargo()

conn.close()