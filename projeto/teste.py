import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb


db_path = "./database/database_cd.db"
conn = duckdb.connect(db_path)


ids_municipios = [431390, 431020, 431410]  # Panambi, Ijuí, Passo Fundo
municipios_nome = {431390: 'Panambi', 431020: 'Ijuí', 431410: 'Passo Fundo'}


def carregar_dados_acidentes(ano):
    query = f"""
    SELECT *
    FROM dadosacidentetrabalho
    WHERE ID_MUNICIP IN ({','.join(map(str, ids_municipios))})
    AND NU_ANO = {ano}
    """
    return pd.read_sql_query(query, conn)

dados_2022 = carregar_dados_acidentes(2022)
dados_2023 = carregar_dados_acidentes(2023)

acidentes_2022 = dados_2022.groupby('ID_MUNICIP').size()
acidentes_2022.index = [municipios_nome[id_] for id_ in acidentes_2022.index]

acidentes_2023 = dados_2023.groupby('ID_MUNICIP').size()
acidentes_2023.index = [municipios_nome[id_] for id_ in acidentes_2023.index]

comparacao = pd.DataFrame({
    '2022': acidentes_2022,
    '2023': acidentes_2023
})

plt.figure(figsize=(10, 6))
comparacao.plot(kind='bar', color=['purple', 'pink'], alpha=0.8)
plt.title('Comparação de Acidentes de Trabalho (2022 vs 2023)')
plt.ylabel('Quantidade de Acidentes')
plt.xlabel('Municípios')
plt.xticks(rotation=0)
plt.legend(title='Ano')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()


def calcular_porcentagem(acidentes):
    total_acidentes = acidentes.sum()
    return (acidentes / total_acidentes) * 100

porcentagem_2022 = calcular_porcentagem(acidentes_2022)
porcentagem_2023 = calcular_porcentagem(acidentes_2023)


plt.figure(figsize=(10, 6))
plt.plot(acidentes_2022.index, acidentes_2022, marker='o', color='purple', label='2022', linestyle='-', linewidth=2, markersize=8)
plt.plot(acidentes_2023.index, acidentes_2023, marker='o', color='hotpink', label='2023', linestyle='-', linewidth=2, markersize=8)

for cidade, porcentagem in porcentagem_2022.items():
    plt.text(cidade, acidentes_2022[cidade], f'{porcentagem:.1f}%', fontsize=12, ha='center', va='bottom', color='black')

for cidade, porcentagem in porcentagem_2023.items():
    plt.text(cidade, acidentes_2023[cidade], f'{porcentagem:.1f}%', fontsize=12, ha='center', va='bottom', color='black')

plt.title('Comparação de Acidentes de Trabalho nas Cidades (2022 vs 2023)', fontsize=14)
plt.xlabel('Cidades', fontsize=12)
plt.ylabel('Quantidade de Acidentes', fontsize=12)
plt.legend(title="Ano", loc="upper left")
plt.grid(True, axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()


conn.close()
