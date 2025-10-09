import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from bcb import PTAX
import io
import logging

# ====== Configuração de Logging ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ====== Constantes ======
REQUIRED_COLUMNS = [
    "Tipo de dívida",
    "Situação da dívida",
    "Valor a liberar ou assumir (na moeda de contratação)",
    "Moeda da contratação, emissão ou assunção"
]

MAPA_MOEDAS = {
    "Real": "BRL",
    "Dólar dos EUA": "USD",
    "Euro": "EUR",
    "Direito Especial - SDR": "XDR",
    "Iene": "JPY"
}

SIMBOLOS_MOEDAS = {
    "Real": "R$",
    "Dólar dos EUA": "US$",
    "Euro": "€",
    "Direito Especial - SDR": "SDR",
    "Iene": "¥"
}

# Intervalos bimestrais do RREO: (início período, fim período, data referência cotação)
# Formato: ((mês_ini, dia_ini), (mês_fim, dia_fim), (mês_ref, dia_ref))
INTERVALOS_RREO = [
    ((3, 30), (5, 29), (2, 28)),   # Mar/Abr → Cotação 28/fev
    ((5, 30), (7, 29), (4, 30)),   # Mai/Jun → Cotação 30/abr
    ((7, 30), (9, 29), (6, 30)),   # Jul/Ago → Cotação 30/jun
    ((9, 30), (11, 29), (8, 31)),  # Set/Out → Cotação 31/ago
    ((11, 30), (1, 29), (10, 31)), # Nov/Dez → Cotação 31/out
    ((1, 30), (3, 29), (12, 31))   # Jan/Fev → Cotação 31/dez (ano anterior)
]

MAX_ARQUIVO_MB = 50

# ====== Inicializa PTAX ======
ptax = PTAX()
ep_cotacao = ptax.get_endpoint('CotacaoMoedaDia')

# ====== Funções Auxiliares ======
def formatar_numero_brasil(valor, casas_decimais=2):
    """
    Formata número no padrão brasileiro: ponto para milhar, vírgula para decimal.
    
    Args:
        valor: Número a ser formatado
        casas_decimais: Quantidade de casas decimais (padrão: 2)
    
    Returns:
        String formatada no padrão brasileiro
    """
    if not isinstance(valor, (int, float)):
        return valor
    
    # Formata com casas decimais e separadores
    formatado = f"{valor:,.{casas_decimais}f}"
    # Converte para padrão brasileiro
    formatado = formatado.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
    return formatado


def validar_csv(df):
    """
    Valida se o CSV possui as colunas necessárias.
    
    Args:
        df: DataFrame a ser validado
    
    Raises:
        ValueError: Se colunas obrigatórias estiverem ausentes
    """
    # Normaliza nomes das colunas
    df.columns = [c.strip() for c in df.columns]
    
    # Verifica colunas ausentes
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        logger.error(f"Colunas ausentes no CSV: {missing}")
        raise ValueError(f"O arquivo CSV não possui as colunas obrigatórias: {', '.join(missing)}")
    
    logger.info("Validação do CSV concluída com sucesso")
    return True


def data_cotacao():
    """
    Determina a data de referência para cotação baseada nos períodos do RREO.
    Retorna o último dia útil do bimestre anterior.
    
    Returns:
        String com data no formato MM/DD/YYYY
    """
    hoje = datetime.today()
    ano = hoje.year
    
    # Busca o intervalo correspondente
    for inicio, fim, referencia in INTERVALOS_RREO:
        mes_ini, dia_ini = inicio
        mes_fim, dia_fim = fim
        mes_ref, dia_ref = referencia
        
        # Ajusta o ano para intervalos que cruzam o ano
        if mes_ini < mes_fim:
            ini = datetime(ano, mes_ini, dia_ini)
            fim_periodo = datetime(ano, mes_fim, dia_fim)
        else:
            ini = datetime(ano, mes_ini, dia_ini)
            fim_periodo = datetime(ano + 1, mes_fim, dia_fim)
        
        # Verifica se a data atual está no intervalo
        if ini <= hoje <= fim_periodo:
            # Dezembro do ano anterior para o intervalo Jan/Fev
            ano_ref = ano if mes_ref != 12 else ano - 1
            data_base = datetime(ano_ref, mes_ref, dia_ref)
            break
    else:
        # Fallback: último dia de fevereiro
        logger.warning("Data atual fora dos intervalos definidos, usando fallback")
        data_base = datetime(ano, 2, 28)
    
    # Ajusta para dia útil (não final de semana)
    while data_base.weekday() >= 5:  # 5=Sábado, 6=Domingo
        data_base -= timedelta(days=1)
        logger.info(f"Ajustando para dia útil: {data_base.strftime('%d/%m/%Y')}")
    
    logger.info(f"Data de referência calculada: {data_base.strftime('%d/%m/%Y')}")
    return data_base.strftime("%m/%d/%Y")


@st.cache_data(ttl=3600)  # Cache por 1 hora
def cotacao_bacen(moeda, data_ref):
    """
    Busca cotação PTAX de compra no Banco Central para uma moeda e data específicas.
    Tenta até 5 dias úteis anteriores se não encontrar na data informada.
    
    Args:
        moeda: Código da moeda (USD, EUR, etc.)
        data_ref: Data de referência no formato MM/DD/YYYY
    
    Returns:
        Tupla (cotação, data_utilizada) ou ("-", "-") se não encontrar
    """
    if moeda == "BRL":
        logger.info("Moeda BRL, retornando cotação 1.0")
        return 1.0, ""
    
    # Tenta buscar cotação nos últimos 5 dias úteis
    for i in range(5):
        dt_busca = (datetime.strptime(data_ref, "%m/%d/%Y") - timedelta(days=i)).strftime("%m/%d/%Y")
        
        try:
            logger.info(f"Buscando cotação {moeda} para {dt_busca}")
            df = ep_cotacao.query().parameters(moeda=moeda, dataCotacao=dt_busca).collect()
            
            if not df.empty:
                # Filtra apenas fechamento PTAX
                fechamento = df[df['tipoBoletim'] == 'Fechamento PTAX']
                
                if not fechamento.empty:
                    cotacao = float(fechamento['cotacaoCompra'].values[-1])
                    data_formatada = datetime.strptime(dt_busca, "%m/%d/%Y").strftime("%d/%m/%Y")
                    logger.info(f"Cotação encontrada: {cotacao} em {data_formatada}")
                    return cotacao, data_formatada
                    
        except Exception as e:
            logger.warning(f"Erro ao buscar cotação {moeda} para {dt_busca}: {e}")
            continue
    
    logger.error(f"Não foi possível obter cotação para {moeda}")
    return "-", "-"


def processar_csv(df_csv):
    """
    Processa o CSV de dívidas, filtra registros relevantes, agrupa por moeda
    e converte valores para BRL usando cotações do Banco Central.
    
    Args:
        df_csv: DataFrame com dados do CSV
    
    Returns:
        DataFrame com resumo processado ou None se não houver registros
    """
    logger.info("Iniciando processamento do CSV")
    
    # Normaliza nomes das colunas
    df_csv.columns = [c.strip() for c in df_csv.columns]
    
    # Filtros aplicados
    logger.info("Aplicando filtros: tipo=empréstimo, situação=vigente, valor>0")
    df_filtrado = df_csv[
        (df_csv["Tipo de dívida"].str.strip().str.lower() == "empréstimo ou financiamento") &
        (df_csv["Situação da dívida"].str.strip().str.lower() == "vigente") &
        (df_csv["Valor a liberar ou assumir (na moeda de contratação)"] > 0)
    ]
    
    if df_filtrado.empty:
        logger.warning("Nenhum registro encontrado após aplicar filtros")
        return None
    
    logger.info(f"Registros encontrados após filtros: {len(df_filtrado)}")
    
    # Agrupa por moeda
    resumo = (
        df_filtrado.groupby("Moeda da contratação, emissão ou assunção")
        ["Valor a liberar ou assumir (na moeda de contratação)"]
        .sum()
        .reset_index()
    )
    
    logger.info(f"Moedas encontradas: {resumo['Moeda da contratação, emissão ou assunção'].tolist()}")
    
    # Obtém data de referência para cotação
    data_ref = data_cotacao()
    
    # Processa cada moeda
    valores = []
    for _, row in resumo.iterrows():
        nome_moeda = row["Moeda da contratação, emissão ou assunção"]
        valor = row["Valor a liberar ou assumir (na moeda de contratação)"]
        
        codigo = MAPA_MOEDAS.get(nome_moeda)
        if not codigo:
            logger.warning(f"Moeda não mapeada: {nome_moeda}")
            continue
        
        # Busca cotação
        cot, data_usada = cotacao_bacen(codigo, data_ref)
        
        # Calcula valor em BRL
        valor_brl = valor * cot if isinstance(cot, float) else valor
        
        valores.append((nome_moeda, valor, cot, data_usada, valor_brl))
        logger.info(f"{nome_moeda}: {valor} × {cot} = R$ {valor_brl:,.2f}")
    
    # Cria DataFrame de saída
    df_saida = pd.DataFrame(
        valores, 
        columns=["Moeda", "Valor a Liberar", "Cotação", "Data da Cotação", "Valor em BRL"]
    )
    
    # Adiciona linha TOTAL
    total_brl = df_saida["Valor em BRL"].sum()
    total = pd.DataFrame(
        [["TOTAL", "", "", "", total_brl]], 
        columns=df_saida.columns
    )
    df_saida = pd.concat([df_saida, total], ignore_index=True)
    
    logger.info(f"Processamento concluído. Total em BRL: R$ {total_brl:,.2f}")
    return df_saida


def formatar_para_exibicao(df_resumo):
    """
    Formata o DataFrame para exibição com símbolos de moeda e padrão brasileiro.
    
    Args:
        df_resumo: DataFrame com dados processados
    
    Returns:
        DataFrame formatado para exibição
    """
    df_vis = df_resumo.copy()
    
    for i, row in df_vis.iterrows():
        moeda = row["Moeda"]
        
        if moeda != "TOTAL":
            # Formata valor a liberar com símbolo da moeda
            simbolo = SIMBOLOS_MOEDAS.get(moeda, "")
            valor_formatado = formatar_numero_brasil(row["Valor a Liberar"], 2)
            df_vis.at[i, "Valor a Liberar"] = f"{simbolo} {valor_formatado}"
            
            # Formata cotação
            if isinstance(row["Cotação"], (int, float)):
                df_vis.at[i, "Cotação"] = formatar_numero_brasil(row["Cotação"], 5)
            
            # Substitui vazio por "-"
            if df_vis.at[i, "Data da Cotação"] == "":
                df_vis.at[i, "Data da Cotação"] = "-"
        else:
            # Linha TOTAL
            df_vis.at[i, "Valor a Liberar"] = "-"
            df_vis.at[i, "Cotação"] = "-"
            df_vis.at[i, "Data da Cotação"] = "-"
        
        # Formata valor em BRL (todas as linhas)
        valor_brl_formatado = formatar_numero_brasil(row["Valor em BRL"], 2)
        df_vis.at[i, "Valor em BRL"] = f"R$ {valor_brl_formatado}"
    
    return df_vis


def gerar_html_tabela(df_vis):
    """
    Gera HTML customizado para exibir a tabela com formatação especial.
    
    Args:
        df_vis: DataFrame formatado para exibição
    
    Returns:
        String com HTML da tabela
    """
    html = """
    <style>
    .dataframe-custom {
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
        font-size: 14px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .dataframe-custom th {
        background-color: #1f77b4;
        color: white;
        padding: 12px;
        text-align: left;
        font-weight: 600;
    }
    .dataframe-custom td {
        padding: 10px 12px;
        border-bottom: 1px solid #ddd;
    }
    .dataframe-custom tr:hover:not(.total-row) {
        background-color: #f5f5f5;
    }
    .total-row {
        font-weight: bold;
        background-color: rgba(31, 119, 180, 0.15) !important;
        border-top: 2px solid #1f77b4;
    }
    .total-row td {
        padding: 12px;
        font-size: 15px;
        color: inherit;
    }
    </style>
    <table class="dataframe-custom">
    <thead>
        <tr>
            <th>Moeda</th>
            <th>Valor a Liberar</th>
            <th>Cotação</th>
            <th>Data da Cotação</th>
            <th>Valor em BRL</th>
        </tr>
    </thead>
    <tbody>
    """
    
    for _, row in df_vis.iterrows():
        classe = 'class="total-row"' if row["Moeda"] == "TOTAL" else ''
        html += f'<tr {classe}>'
        html += f'<td>{row["Moeda"]}</td>'
        html += f'<td>{row["Valor a Liberar"]}</td>'
        html += f'<td>{row["Cotação"]}</td>'
        html += f'<td>{row["Data da Cotação"]}</td>'
        html += f'<td>{row["Valor em BRL"]}</td>'
        html += '</tr>'
    
    html += "</tbody></table>"
    return html


# ====== Interface Streamlit ======
st.set_page_config(
    page_title="Resumo de Dívidas CDP",
    page_icon="💱",
    layout="centered"
)

st.title("💱 Gerar resumo do valor a liberar das dívidas no CDP")

# Upload do arquivo
uploaded_file = st.file_uploader(
    "Faça upload do arquivo [...]02-dividas.csv exportado do CDP do EF no SADIPEM",
    type="csv",
    help="Selecione o arquivo [...]02-dividas.csv"
)

if uploaded_file:
    # Verifica tamanho do arquivo
    file_size_mb = uploaded_file.size / (1024 * 1024)
    if file_size_mb > MAX_ARQUIVO_MB:
        st.error(f"❌ Arquivo muito grande ({file_size_mb:.1f}MB). Tamanho máximo: {MAX_ARQUIVO_MB}MB")
        st.stop()
    
    try:
        with st.spinner('🔄 Processando arquivo...'):
            # Lê o arquivo CSV
            logger.info(f"Lendo arquivo CSV: {uploaded_file.name} ({file_size_mb:.2f}MB)")
            df_csv = pd.read_csv(
                uploaded_file, 
                sep=";", 
                encoding="cp1252", 
                thousands=".", 
                decimal=","
            )
            
            # Valida estrutura
            validar_csv(df_csv)
            
            # Processa os dados
            df_resumo = processar_csv(df_csv)
            
            # Verifica se encontrou registros
            if df_resumo is None:
                st.warning("⚠️ **Nenhum registro encontrado que atenda aos critérios**")
                st.info("""
                O arquivo foi processado, mas não foram encontrados registros que atendam aos seguintes critérios:
                
                - **Tipo de dívida**: "Empréstimo ou financiamento"
                - **Situação da dívida**: "Vigente"
                - **Valor a liberar**: Maior que zero
                """)
                st.stop()
            
            # Formata para exibição
            df_vis = formatar_para_exibicao(df_resumo)
        
        # Exibe resultado
        st.success("✅ Processamento concluído com sucesso!")
        
        st.subheader("📊 Valor a Liberar por Moeda e Total em Reais")
        
        # Exibe tabela HTML customizada
        html_tabela = gerar_html_tabela(df_vis)
        st.markdown(html_tabela, unsafe_allow_html=True)
        
        # Botão de download
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with io.BytesIO() as buffer:
                df_resumo.to_excel(buffer, index=False, engine='openpyxl')
                st.download_button(
                    label="📥 Download em Excel (.xlsx)",
                    data=buffer.getvalue(),
                    file_name=f"resumo_dividas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        
        # Informações adicionais
        st.divider()
        st.caption(f"📅 Processado em: {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}")
        st.caption(f"📄 {len(df_resumo) - 1} moeda(s) utilizada(s)")
        
    except ValueError as ve:
        st.error(f"❌ Erro de validação: {ve}")
        logger.error(f"Erro de validação: {ve}")
        
    except Exception as e:
        st.error(f"❌ Erro ao processar o arquivo: {e}")
        logger.exception("Erro durante processamento")
        st.info("💡 Verifique se o arquivo está no formato correto exportado do CDP")

# Informações e instruções
with st.expander("ℹ️ Instruções de Uso"):
    st.markdown("""
    ### Como usar este aplicativo:
    
    1. **Fazer upload**: Faça o upload do arquivo `[...]02-dividas.csv` exportado do CDP do Ente Federativo no SADIPEM
    
    2. **Processamento automático**: O sistema irá:
       - Filtrar apenas dívidas do tipo "Empréstimo ou financiamento"
       - Considerar somente dívidas com situação "Vigente"
       - Incluir apenas valores a liberar maiores que zero
       - Agrupar os valores por moeda
       - Converter para Real (BRL) usando cotações oficiais PTAX do Banco Central
    
    3. **Download**: Após o processamento, faça o download da planilha Excel com os resultados
    
    ---
    
    ### Sobre as cotações:
    **Fonte**: Os valores serão convertidos para Real utilizando a cotação PTAX de compra do Banco Central, referente ao fechamento do dia
    
    **Data da cotação**: A data da cotação é o último dia do RREO exigível (último dia do bimestre) ou data útil anterior caso caia em final de semana ou feriado
    
    ---
    
    ### Moedas suportadas:
    - Real (BRL)
    - Dólar dos EUA (USD)
    - Euro (EUR)
    - Direito Especial de Saque - SDR (XDR)
    - Iene (JPY)

    """)

with st.expander("🔧 Informações Técnicas"):
    st.markdown("""
    ### Tecnologias utilizadas:
    - **Streamlit**: Interface web interativa
    - **Pandas**: Processamento e análise de dados
    - **BCB (python-bcb)**: Integração com API do Banco Central
    - **Python 3.x**: Linguagem de programação
    
    ### Critérios de filtragem:
    ```
    Tipo de dívida = "Empréstimo ou financiamento"
    Situação da dívida = "Vigente"
    Valor a liberar > 0
    ```
    
    ### Logs e cache:
    - Sistema de logs configurado para rastreabilidade
    - Cache de cotações por 1 hora (reduz chamadas à API)
    - Validações em múltiplas etapas do processamento
    """)

# Rodapé
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; font-size: 12px;'>
    <p>Cotações fornecidas pelo Banco Central do Brasil via API PTAX</p>
</div>
""", unsafe_allow_html=True)