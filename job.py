import pyodbc
import schedule
import configparser
from datetime import datetime
import tkinter as tk
from tkinter import messagebox, ttk
import os
import threading


# Cores da interface
COLORS = {
    "bg_main": "#f0f2f5",
    "bg_card": "#ffffff",
    "primary": "#1a73e8",
    "primary_hover": "#1557b0",
    "success": "#34a853",
    "text_primary": "#202124",
    "text_secondary": "#5f6368",
    "border": "#dadce0",
    "debug": "#a84e3b",
    "debug1": "#70f027",
}

# Variáveis globais
server = ""
database = ""
username = "VMDApp"
password = "VMD22041748"
cod_loja = ""
cod_cliente = 0
cod_politica = 0
sistema = "VAREJO"
X_MINUTOS = 0
last_run_time = None
running_job = False

def load_config():
    """Carrega as configurações de um arquivo INI e inicializa as variáveis globais."""
    global server, database, username, password, cod_loja, X_MINUTOS, cod_cliente, cod_politica, sistema

    config = configparser.ConfigParser()
    
    # Verifica se o arquivo config.ini existe, se não, cria com valores padrão
    if not os.path.exists("config.ini"):
        config['CONFIG'] = {
            'SERVER': 'localhost',
            'DATABASE': 'sua_base_de_dados',
            'INTERVALO_MINUTOS': '5',
            'COD_LOJA': '1',
            'SISTEMA': 'VAREJO', # ou ATACADO
            'CLIENTE': '0', # Apenas para ATACADO
            'IDPOLCOM': '0' # Apenas para ATACADO
        }
        with open("config.ini", 'w') as configfile:
            config.write(configfile)
        messagebox.showinfo("Arquivo de Configuração Criado", "O arquivo 'config.ini' não foi encontrado e foi criado com valores padrão. Por favor, edite-o com suas configurações.")

    config.read("config.ini")
    try:
        server = config.get("CONFIG", "SERVER")
        database = config.get("CONFIG", "DATABASE")
        X_MINUTOS = config.getint("CONFIG", "INTERVALO_MINUTOS")
        cod_loja = config.get("CONFIG", "COD_LOJA")
        sistema = config.get("CONFIG", "SISTEMA", fallback="VAREJO")
        
        if sistema == "ATACADO":
            cod_cliente = config.get("CONFIG", "CLIENTE")
            cod_politica = config.get("CONFIG", "IDPOLCOM")
            username = "DMDApp"
            password = "DMD20051643"

        
    except Exception as e:
        messagebox.showerror("Erro de Configuração", f"Erro ao carregar configurações: {e}")
        exit(1)

def connect_to_database():
    """Estabelece uma conexão com o banco de dados SQL Server."""
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )
        return connection
    except Exception as e:
        return None

def fetch_data(cod_loja):
    """Executa a consulta SQL e retorna os resultados."""
    if sistema == "VAREJO":
        query = f"""
        SELECT
            PREAN.Cod_Ean as codigo,
            LEFT(PRODU.Des_Produt,30) as nome,
            FORMAT(ISNULL(PRXLJ.Prc_VenAtu,0),'N2','pt-BR') as preco1,
            FORMAT(ISNULL(PRMIT.Prc_Promoc,0),'N2','pt-BR') as preco2
        FROM PREAN
            INNER JOIN PRODU
                ON PREAN.Cod_Produt = PRODU.Cod_Produt
            INNER JOIN PRXLJ
                ON PREAN.Cod_Produt = PRXLJ.Cod_Produt
                AND PRXLJ.Cod_Loja={cod_loja}
            LEFT JOIN PRMIT
                ON PRXLJ.Cod_PrmAtv = PRMIT.Cod_Promoc
                AND PRXLJ.Cod_Produt = PRMIT.Cod_Produt
                AND PRXLJ.Cod_loja = PRMIT.Cod_loja
        ORDER BY 2
        """
    else: 
        query = f"""
        Declare @IdPolCom Int,@CodCli Int, @CodEstab Int 
                          set @IdPolCom = {cod_politica}
                          set @CodCli = {cod_cliente}
                          set @CodEstab = {cod_loja}
                          Select
                            pr.Cod_Ean as codigo, 
                            LEFT(pr.Descricao,30) as nome, 
                            preco1 = FORMAT(CONVERT(MONEY, ISNULL(
                                -- Tenta pegar a promoção primeiro, senão pega o 1º escalonamento (Nivel ASC), senão o preço base
                                COALESCE(
                                    (SELECT TOP 1 Prc_Promoc FROM dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo, getDate()) WHERE IsNull(ES.Flg_BlqVen, 0) = 0 AND Prc_Promoc > 0 ORDER BY Nivel, Ordem DESC),
                                    (SELECT TOP 1 Prc_Promoc FROM dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo, getDate()) WHERE IsNull(ES.Flg_BlqVen, 0) = 0 ORDER BY Nivel ASC),
                                    ES.Prc_Venda
                                ) * (1-Iif((Select top 1 Per_DscVis from dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo ,getDate()) Where IsNull(ES.Flg_BlqVen, 0) = 0 Order by Nivel, Ordem desc)>0, 
                                            (Select top 1 Per_DscVis from dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo ,getDate()) Where IsNull(ES.Flg_BlqVen, 0) = 0 Order by Nivel, Ordem desc), 
                                     Iif((Select top 1 Per_Descon from dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo ,getDate()) Where IsNull(ES.Flg_BlqVen, 0) = 0 Order by Nivel, Ordem desc)>0, 
                                          (Select top 1 Per_Descon from dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo ,getDate()) Where IsNull(ES.Flg_BlqVen, 0) = 0 Order by Nivel, Ordem desc), 
                                        ES.Per_DscAut))/100),0)), 'N2', 'pt-BR'), 
                           preco2 = FORMAT(ISNULL((SELECT TOP 1 Prc_Promoc FROM dbo.FN_ViewPoliticasProduto(po.Id_PolCom, pr.codigo, getDate()) WHERE IsNull(ES.Flg_BlqVen, 0) = 0 AND Prc_Promoc > 0 ORDER BY Nivel, Ordem DESC), 0), 'N2', 'pt-BR') 
                           From PRODU pr 
                          Inner Join PRXES ES on (Es.Cod_Produt = Codigo and Es.Cod_Estabe = @CodEstab) 
                          Inner Join FABRI fb on fb.Codigo = pr.Cod_Fabricante 
                          Left Join SBBAS sb on sb.Codigo = pr.Cod_SubBas  
                          left join (
                          select p.Id_PolCom  
                                from POCOM p  
                                      Inner Join PCXES pe on ((p.Id_PolCom = pe.Id_PolCom) and (pe.Cod_Estabe = @CodEstab))  
                                      Left Join TBPRC tb on ((tb.Cod_Estabe = @CodEstab) or (tb.Flg_AllEstabe = 1)) and  
                                                            (p.Cod_TabPrc = tb.Cod_TabPrc)  
                                      Left Join TPXPR tp on ( (tb.Cod_Estabe = tp.Cod_Estabe) and (tb.Cod_TabPrc = tp.Cod_TabPrc) and (tp.Cod_Produt = Cod_Produt) )  
                                      Left Join CLIEN cl on cl.Codigo = @CodCli 
                                      Left Join PCXCL pc on ((p.Id_PolCom = pc.Id_PolCom) and (pc.Cod_Client = cl.Codigo))  
                                      Left Join PCXGC pg on ((p.Id_PolCom = pg.Id_PolCom) and (pg.Cod_GrpCli = cl.Cod_GrpCli))   
                                      Left Join PCXUF pu on ((p.Id_PolCom = pu.Id_PolCom) and (pu.Cod_Uf = cl.Cod_Estado))  
                                  where ((pc.Cod_Client > 0) or (pg.Cod_GrpCli > 0) or (pu.Cod_Uf <> ''))  
                                    and dbo.FN_TransacaoAtiva(p.Dat_Inicio, p.Dat_Termino, getDate(), p.Bloqueado) = 1 
                                    and Substring(IsNull(p.Tip_PolCom,''),3,1) <> 'K'  
                                    and ((p.Flg_BlqCli = 0) or ((p.Flg_BlqCli = 1) and (IsNull(cl.Flg_BlqPrm,0) = 0))) 
                                    and p.Flg_Balcao = 1 
                          ) po on po.Id_PolCom = @IdPolCom 
                          where isnull(cod_ean,'') != ''
                          ORDER BY 1 desc
        """
    connection = connect_to_database()
    if not connection:
        return []

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        connection.close()
        return rows
    except Exception as e:
        return []

def create_txt_file(data):
    """Cria um arquivo TXT com os dados formatados."""
    filename = "output.txt"
    try:
        with open(filename, "w") as file:
            for row in data:
                line = f"{row.codigo}|{row.nome}|{row.preco1}|{row.preco2}\n"
                file.write(line)
        return True
    except Exception as e:
        return False

def job(cod_loja=None):
    """Função agendada para rodar o processo completo."""
    global last_run_time, running_job
    
    if running_job:
        return
        
    running_job = True
    update_status("Conectando ao banco de dados...")
    
    # Se não for especificado, usa o valor global
    if cod_loja is None:
        cod_loja = globals()['cod_loja']
        
    # Atualiza status de execução
    progress_bar.start(10)
    progress_value.set(25)
    root.update_idletasks()
    
    update_status("Consultando produtos...")
    data = fetch_data(cod_loja)
    
    if data:
        progress_value.set(75)
        update_status(f"Processando {len(data)} produtos...")
        root.update_idletasks()
        
        if create_txt_file(data):
            last_run_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            progress_value.set(100)
            update_status(f"Concluído! {len(data)} produtos exportados.")
            update_ui()
        else:
            update_status("Erro ao criar arquivo de saída!")
    else:
        update_status("Erro na consulta ou nenhum dado retornado.")
    
    progress_bar.stop()
    running_job = False

def update_ui():
    """Atualiza os elementos da interface gráfica."""
    config_text = f"{sistema}"
    
    if sistema == "VAREJO":
        config_text += f" • Loja {cod_loja}"
    else:
        config_text += f" • Estabelecimento {cod_loja}"
        config_text += f" • Cliente {cod_cliente}"
        config_text += f" • Política {cod_politica}"
    
    lbl_config_value.config(text=config_text)
    lbl_server_value.config(text=f"{server}")
    lbl_database_value.config(text=f"{database}")
    lbl_interval_value.config(text=f"A cada {X_MINUTOS} minutos")
    
    if last_run_time:
        lbl_last_run_value.config(text=last_run_time)
        lbl_status_icon.config(text="✓", fg=COLORS["success"])
    else:
        lbl_last_run_value.config(text="Ainda não executado")
        lbl_status_icon.config(text="◯", fg=COLORS["text_secondary"])

def manual_job():
    """Executa o job manualmente em uma thread separada."""
    if running_job:
        messagebox.showinfo("Aviso", "Um processo já está em execução!")
        return
        
    threading.Thread(target=job).start()

def update_status(message):
    """Atualiza a mensagem de status na interface."""
    lbl_status.config(text=message)
    root.update_idletasks()

def test_connection():
    """Testa a conexão com o banco de dados."""
    update_status("Testando conexão...")
    connection = connect_to_database()
    if connection:
        connection.close()
        messagebox.showinfo("Conexão", "Conexão com o banco de dados estabelecida com sucesso!")
        update_status("Conexão bem-sucedida.")
    else:
        messagebox.showerror("Erro de Conexão", "Não foi possível conectar ao banco de dados!")
        update_status("Falha na conexão.")

def create_custom_button(parent, text, command, **kwargs):
    """Cria um botão personalizado com efeitos de hover."""
    style = kwargs.get("style", "primary")
    width = kwargs.get("width", 150)
    
    if style == "primary":
        bg_color = COLORS["primary"]
        hover_color = COLORS["primary_hover"]
        fg_color = "white"
    else:
        bg_color = COLORS["bg_card"]
        hover_color = COLORS["border"]
        fg_color = COLORS["text_primary"]
    
    button = tk.Button(
        parent,
        text=text,
        command=command,
        bg=bg_color,
        fg=fg_color,
        relief=tk.FLAT,
        width=width // 10,  # Ajusta para unidades de texto
        pady=8,
        cursor="hand2",
        font=("Segoe UI", 9)
        
    )
    
    def on_enter(e):
        button['background'] = hover_color
        
    def on_leave(e):
        button['background'] = bg_color
        
    button.bind("<Enter>", on_enter)
    button.bind("<Leave>", on_leave)
    
    return button

def create_card(parent, title, **kwargs):
    """Cria um card com título e conteúdo."""
    card = tk.Frame(
        parent,
        bg=COLORS["bg_card"],
        highlightbackground=COLORS["border"],
        highlightthickness=1,
        padx=15,
        pady=2
    )
    
    if title:
        lbl_title = tk.Label(
            card,
            text=title,
            font=("Segoe UI", 10, "bold"),
            bg=COLORS["bg_card"],
            fg=COLORS["text_primary"]
        )
        lbl_title.pack(anchor="w", pady=(4, 4))
    
    return card

def scheduler_loop():
    """Executa o agendador periodicamente."""
    schedule.run_pending()
    root.after(1000, scheduler_loop)

# Carrega configurações no início
load_config()
schedule.every(X_MINUTOS).minutes.do(lambda: threading.Thread(target=job).start())

# Criação da interface gráfica
root = tk.Tk()
root.title("Gerador de Produtos • Exportação TXT")
root.geometry("500x500")
root.configure(bg=COLORS["bg_main"])
root.resizable(False, False)

# Configurar fonte padrão
font_default = ("Segoe UI", 10)
font_title = ("Segoe UI", 16, "bold")
font_subtitle = ("Segoe UI", 12, "bold")
font_value = ("Segoe UI", 10)

# Container principal
main_container = tk.Frame(root, bg=COLORS["bg_main"], padx=20, pady=2)
main_container.pack(fill=tk.BOTH, expand=True)

# Card das informações de conexão
conn_card = create_card(main_container, "Configurações")
conn_card.pack(fill=tk.X, pady=2)

# Grid de informações
info_frame = tk.Frame(conn_card, bg=COLORS["bg_card"])
info_frame.pack(fill=tk.X)

# Primeira coluna - informações de conexão
conn_col = tk.Frame(info_frame, bg=COLORS["bg_card"], padx=10)
conn_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

# Servidor
lbl_server = tk.Label(
    conn_col,
    text="Servidor:",
    font=font_default,
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_server.grid(row=0, column=0, sticky="w", pady=3)

lbl_server_value = tk.Label(
    conn_col,
    text=server,
    font=font_value,
    bg=COLORS["bg_card"],
    fg=COLORS["text_primary"]
)
lbl_server_value.grid(row=0, column=1, sticky="w", padx=10, pady=3)

# Banco de dados
lbl_database = tk.Label(
    conn_col,
    text="Banco de dados:",
    font=font_default,
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_database.grid(row=1, column=0, sticky="w", pady=3)

lbl_database_value = tk.Label(
    conn_col,
    text=database,
    font=font_value,
    bg=COLORS["bg_card"],
    fg=COLORS["text_primary"]
)
lbl_database_value.grid(row=1, column=1, sticky="w", padx=10, pady=3)

# Segunda coluna - informações de configuração
config_col = tk.Frame(info_frame, bg=COLORS["bg_card"], padx=10)
config_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

# Configuração
lbl_config = tk.Label(
    conn_col,
    text="Sistema:",
    font=font_default,
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_config.grid(row=2, column=0, sticky="w", pady=3)

lbl_config_value = tk.Label(
    conn_col,
    text=f"{sistema} • Loja {cod_loja}",
    font=font_value,
    bg=COLORS["bg_card"],
    fg=COLORS["text_primary"]
)
lbl_config_value.grid(row=2, column=1, sticky="w", padx=10, pady=3)

# Intervalo
lbl_interval = tk.Label(
    conn_col,
    text="Intervalo:",
    font=font_default,
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_interval.grid(row=3, column=0, sticky="w", pady=3)

lbl_interval_value = tk.Label(
    conn_col,
    text=f"A cada {X_MINUTOS} minutos",
    font=font_value,
    bg=COLORS["bg_card"],
    fg=COLORS["text_primary"]
)
lbl_interval_value.grid(row=3, column=1, sticky="w", padx=10, pady=3)

# Botão de teste de conexão
btn_test = create_custom_button(
    conn_card,
    "Testar Conexão",
    test_connection,
    style="secondary",
    width=120
)
btn_test.pack(anchor="e", pady=(15, 0))

# Card de status e operações
status_card = create_card(main_container, "Status da Execução")
status_card.pack(fill=tk.X, pady=10)

# Status de última execução
status_frame = tk.Frame(status_card, bg=COLORS["bg_card"])
status_frame.pack(fill=tk.X, pady=(0, 10))

# Ícone de status
lbl_status_icon = tk.Label(
    status_frame,
    text="◯",
    font=("Segoe UI", 20),
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_status_icon.pack(side=tk.LEFT, padx=(0, 10))

# Container de última execução
last_run_container = tk.Frame(status_frame, bg=COLORS["bg_card"])
last_run_container.pack(side=tk.LEFT, fill=tk.X)

lbl_last_run = tk.Label(
    last_run_container,
    text="Última execução:",
    font=font_default,
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_last_run.pack(anchor="w")

lbl_last_run_value = tk.Label(
    last_run_container,
    text="Ainda não executado",
    font=font_value,
    bg=COLORS["bg_card"],
    fg=COLORS["text_primary"]
)
lbl_last_run_value.pack(anchor="w")

# Barra de progresso
progress_frame = tk.Frame(status_card, bg=COLORS["bg_card"], pady=10)
progress_frame.pack(fill=tk.X)

progress_value = tk.IntVar(value=0)
progress_bar = ttk.Progressbar(
    progress_frame,
    orient="horizontal",
    length=200,
    mode="determinate",
    variable=progress_value
)
progress_bar.pack(fill=tk.X)

# Status atual
lbl_status = tk.Label(
    progress_frame,
    text="Aguardando execução...",
    font=font_default,
    bg=COLORS["bg_card"],
    fg=COLORS["text_secondary"]
)
lbl_status.pack(pady=(5, 0), anchor="w")

# Botão de execução manual
action_frame = tk.Frame(status_card, bg=COLORS["bg_card"], pady=10)
action_frame.pack(fill=tk.X)

btn_generate = create_custom_button(
    action_frame,
    "Gerar Arquivo Agora",
    manual_job,
    width=200
)
btn_generate.pack(anchor="center", pady=(5, 0))

# Rodapé
footer_frame = tk.Frame(root, bg=COLORS["bg_main"], pady=10)
footer_frame.pack(fill=tk.X, side=tk.BOTTOM)

lbl_footer = tk.Label(
    footer_frame,
    text=f"© {datetime.now().year} • Gerador de Produtos v25.01",
    font=("Segoe UI", 8),
    bg=COLORS["bg_main"],
    fg=COLORS["text_secondary"]
)
lbl_footer.pack()

# Configuração de estilos para ttk
style = ttk.Style()
style.theme_use('clam')
style.configure(
    "TProgressbar",
    troughcolor=COLORS["border"],
    background=COLORS["primary"],
    thickness=8
)

# Inicia o loop do agendador e da interface
scheduler_loop()
update_ui()
root.mainloop()