import pandas as pd
import matplotlib.pyplot as plt
import duckdb

csv_path = "./OCUPANET.csv"
db_path = "./database/database_cd.db"

# -*- coding: utf-8 -*-
conn = duckdb.connect(db_path)

ids_municipios = [431390, 431020, 431410]  
municipios_nome = {431390: 'Panambi', 431020: 'Ijuí', 431410: 'Passo Fundo'}

cargos = pd.read_csv(csv_path, encoding='ISO-8859-1')
cargos.set_index('ID_OCUPA_N', inplace=True)

def carregar_dados_acidentes(ano):
    query = f"""
    SELECT ID_MUNICIP, ID_OCUPA_N
    FROM dadosacidentetrabalho
    WHERE ID_MUNICIP IN ({','.join(map(str, ids_municipios))})
    AND NU_ANO = {ano}
    """
    return pd.read_sql_query(query, conn)

dados_2022 = carregar_dados_acidentes(2022)
dados_2023 = carregar_dados_acidentes(2023)

dados_2022['Cargo'] = dados_2022['ID_OCUPA_N'].map(cargos['Descricao'])
dados_2023['Cargo'] = dados_2023['ID_OCUPA_N'].map(cargos['Descricao'])

cargos_acidentes_2022 = dados_2022['Cargo'].value_counts()
cargos_acidentes_2023 = dados_2023['Cargo'].value_counts()

fig, axes = plt.subplots(1, 2, figsize=(16, 8), sharey=True)

cargos_acidentes_2022.head(10).plot(kind='bar', ax=axes[0], color='purple', alpha=0.8)
axes[0].set_title('Cargos Mais Afetados (2022)')
axes[0].set_ylabel('Número de Acidentes')
axes[0].set_xlabel('Cargo')
axes[0].tick_params(axis='x', rotation=45)

cargos_acidentes_2023.head(10).plot(kind='bar', ax=axes[1], color='pink', alpha=0.8)
axes[1].set_title('Cargos Mais Afetados (2023)')
axes[1].set_xlabel('Cargo')
axes[1].tick_params(axis='x', rotation=45)

plt.suptitle('Cargos com Mais Acidentes de Trabalho', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.show()

comparacao_cargos = pd.DataFrame({
    '2022': cargos_acidentes_2022,
    '2023': cargos_acidentes_2023
}).fillna(0)

comparacao_cargos.head(10).plot(kind='bar', figsize=(12, 6), color=['purple', 'pink'], alpha=0.8)
plt.title('Comparação de Acidentes por Cargo (2022 vs 2023)')
plt.xlabel('Cargo')
plt.ylabel('Número de Acidentes')
plt.xticks(rotation=45)
plt.legend(title='Ano')
plt.tight_layout()
plt.show()

conn.close()
