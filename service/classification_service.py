import pandas as pd
import io
import re
import csv

def classifica_comentarios(df, df_info, survey_id):

    # Informar o id das perguntas abertas e perguntas que contém comentário
    perguntas_abertas = [1,18]
    nova_planilha = []
    colunas = df.columns.tolist()
    i = 0

    while i < len(colunas):
        coluna_atual = colunas[i]

        match = re.match(r'^(\d+)-ID-Categoria$', coluna_atual)
        if match:
            numero_pergunta = int(match.group(1))

            if (not perguntas_abertas or numero_pergunta in perguntas_abertas) and i + 1 < len(colunas):

                # Definição dos nomes das colunas
                coluna_categoria = coluna_atual
                coluna_pergunta = colunas[i + 1]

                # Verificar se existe a coluna de comentário
                coluna_comentario = None
                if (i + 2) < len(colunas):
                    proxima_coluna = colunas[i + 2]
                    if proxima_coluna.startswith(f"{numero_pergunta}-Comentario"):
                        coluna_comentario = proxima_coluna

                pergunta_texto = re.sub(r'^\d+-', '', coluna_pergunta.strip())

                for idx, row in df.iterrows():
                    email = row['Email']
                    resposta = row[coluna_pergunta]
                    categoria = row[coluna_categoria]

                    # Remove o número e hífen no início da categoria
                    categoria = re.sub(r'^\d+-', '', str(categoria).strip())

                    # Verifica se existe comentário
                    if coluna_comentario:
                        comentario = row.get(coluna_comentario, None)
                        comentario_valido = pd.notna(comentario) and str(comentario).strip() != ""

                        if comentario_valido:
                            resposta = comentario  # Usa o comentário
                        else:
                            # Comentário existe, mas está vazio → ignora o registro
                            continue
                    else:
                        # Comentário não existe → aplica a regra padrão de resposta
                        if pd.isna(resposta) or str(resposta).strip() == "":
                            continue  # Ignora se a resposta estiver vazia

                    nova_planilha.append({
                        'email': email,
                        'categoria': categoria,
                        'pergunta': pergunta_texto,
                        'resposta': resposta
                    })

                # Pular para a próxima pergunta (ID-Categoria + Pergunta [+ Comentario se houver])
                i += 2
                if coluna_comentario:
                    i += 1
            else:
                i += 1
        else:
            i += 1

    # Cria o dataframe resultado
    df_resultado = pd.DataFrame(nova_planilha)

    # Limpa emails internos mindsight
    df_resultado = df_resultado[~df_resultado["email"].str.contains("@mindsight.com.br", na=False)]
    df_final = pd.merge(df_resultado, df_info[['email','área', 'gestor']], on='email', how='left')
    return(df_final)
