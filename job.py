import pyodbc
import os
import json
from datetime import datetime

class GeradorArquivoGertec506E:
    """
    Classe responsável por gerar arquivo de produtos para Terminal Gertec 506E
    Formato: EAN|DESCRICAO|PRECO_TOTAL|PRECO_PROMOCAO\r
    """

    def __init__(self, connection_string):
        """
        Inicializa a conexão com o banco de dados
        """
        self.conn = None
        self.cursor = None
        try:
            self.conn = pyodbc.connect(connection_string)
            self.cursor = self.conn.cursor()
            print("✓ Conexão estabelecida com sucesso!")
        except Exception as e:
            print(f"✗ Erro ao conectar ao banco: {str(e)}")
            raise

    def buscar_produtos_com_precos(self, id_polcom, cod_cli, cod_estab):
        """
        Busca todos os produtos com preços calculados usando a política comercial

        Args:
            id_polcom: ID da Política Comercial
            cod_cli: Código do Cliente
            cod_estab: Código do Estabelecimento

        Returns:
            list: Lista de dicionários com os dados dos produtos
        """
        query = """
        -- Parâmetros da política comercial
        DECLARE @IdPolCom INT = ?; 
        DECLARE @CodCli INT = ?;    
        DECLARE @CodEstab INT = ?;      

        SELECT
            pr.Cod_Ean AS [Código EAN], 
            LEFT(pr.Descricao, 30) AS [Nome Produto],

            -- Preço Base da Filial
            ES.Prc_Venda AS [Preço Base],

            -- Valor da Promoção (Se houver Prc_Promoc > 0 no escalonamento)
            (SELECT TOP 1 Prc_Promoc 
             FROM dbo.FN_ViewPoliticasProduto(@IdPolCom, pr.Codigo, GETDATE())
             WHERE IsNull(ES.Flg_BlqVen, 0) = 0 AND Prc_Promoc > 0 
             ORDER BY Nivel, Ordem DESC) AS [Valor Promoção],

            -- Primeiro Escalonamento (Preço por quantidade/nível 1)
            (SELECT TOP 1 Prc_Promoc 
             FROM dbo.FN_ViewPoliticasProduto(@IdPolCom, pr.Codigo, GETDATE())
             WHERE IsNull(ES.Flg_BlqVen, 0) = 0 
             ORDER BY Nivel ASC) AS [1º Escalonamento],

            -- Preço Final calculado (Lógica: Promoção > Escalonamento > Venda)
            ISNULL(
                COALESCE(
                    (SELECT TOP 1 Prc_Promoc FROM dbo.FN_ViewPoliticasProduto(@IdPolCom, pr.Codigo, GETDATE()) 
                     WHERE IsNull(ES.Flg_BlqVen, 0) = 0 AND Prc_Promoc > 0 ORDER BY Nivel, Ordem DESC),
                    (SELECT TOP 1 Prc_Promoc FROM dbo.FN_ViewPoliticasProduto(@IdPolCom, pr.Codigo, GETDATE()) 
                     WHERE IsNull(ES.Flg_BlqVen, 0) = 0 ORDER BY Nivel ASC),
                    ES.Prc_Venda
                ) * (1 - ISNULL((SELECT TOP 1 Per_DscVis FROM dbo.FN_ViewPoliticasProduto(@IdPolCom, pr.Codigo, GETDATE()) 
                                 WHERE IsNull(ES.Flg_BlqVen, 0) = 0 ORDER BY Nivel, Ordem DESC), 0) / 100)
            , 0) AS [Preço Final],

            -- Dados adicionais da tabela PREAN
            PREAN.Cod_Produt,
            PREAN.Tip_Cod,
            PREAN.Qtd_UndEmb,
            PREAN.Des_UndEmb,
            ISNULL(PREAN.Qtd_FraVen, 1) AS Qtd_FraVen,
            PREAN.Pes_Emb,
            PREAN.Alt_Emb,
            PREAN.Lrg_Emb,
            PREAN.Prf_Emb,
            PREAN.Qtd_EmbPalete,
            PREAN.Qtd_CamPalete,
            PREAN.Vol_Emb

        FROM PRODU pr 
        INNER JOIN PRXES ES ON (ES.Cod_Produt = pr.Codigo AND ES.Cod_Estabe = @CodEstab)
        LEFT JOIN PREAN ON (PREAN.Cod_EAN = pr.Cod_Ean)
        WHERE ISNULL(pr.Cod_Ean, '') != ''
        ORDER BY pr.Descricao ASC;
        """

        print(f"\n🔍 Buscando produtos...")
        print(f"   Política Comercial: {id_polcom}")
        print(f"   Cliente: {cod_cli}")
        print(f"   Estabelecimento: {cod_estab}")

        self.cursor.execute(query, (id_polcom, cod_cli, cod_estab))
        colunas = [column[0] for column in self.cursor.description]

        resultados = []
        for row in self.cursor.fetchall():
            resultados.append(dict(zip(colunas, row)))

        print(f"✓ {len(resultados)} produtos encontrados")
        return resultados

    def calcular_preco_com_quantidade(self, produto):
        """
        Calcula o preço multiplicado pela Qtd_FraVen

        Lógica:
        - Se Qtd_FraVen > 0: Preço * Qtd_FraVen (ex: 25.90 * 60 = 1554.00)
        - Se Qtd_FraVen = 0 ou NULL: Preço * 1 (preço unitário)

        Prioridade de preço: Valor Promoção > 1º Escalonamento > Preço Final

        Args:
            produto: Dicionário com os dados do produto

        Returns:
            tuple: (preco_unitario, quantidade, preco_total, tipo_preco)
        """
        # Obtém a quantidade (padrão 1 se não houver)
        qtd_fravem = produto.get('Qtd_FraVen', 1)
        if qtd_fravem is None or qtd_fravem <= 0:
            qtd_fravem = 1

        # Define o preço unitário (prioridade: Promoção > Escalonamento > Final)
        valor_promocao = produto.get('Valor Promoção')
        primeiro_escalonamento = produto.get('1º Escalonamento')
        preco_final = produto.get('Preço Final', 0)

        if valor_promocao and valor_promocao > 0:
            preco_unitario = float(valor_promocao)
            tipo_preco = "PROMOÇÃO"
        elif primeiro_escalonamento and primeiro_escalonamento > 0:
            preco_unitario = float(primeiro_escalonamento)
            tipo_preco = "ESCALONAMENTO"
        else:
            preco_unitario = float(preco_final) if preco_final else 0.0
            tipo_preco = "NORMAL"

        # Calcula o preço total
        preco_total = preco_unitario * qtd_fravem

        return preco_unitario, qtd_fravem, preco_total, tipo_preco

    def gerar_arquivo_output_pipe(self, dados, nome_arquivo='output.txt', pasta_destino='saida_gertec'):
        """
        Gera arquivo no formato exato solicitado:
        EAN|DESCRICAO|PRECO_TOTAL|PRECO_PROMOCAO\r

        Exemplo:
        9555002100025|LUVAS P/PROCED SUPERMAX "P" C/|23,17|0,00
        9500007254433|COLETOR KIT URINA TUBO 12ML+BA|0,00|0,00

        Args:
            dados: Lista de dicionários com os dados dos produtos
            nome_arquivo: Nome do arquivo a ser gerado
            pasta_destino: Pasta onde o arquivo será salvo

        Returns:
            str: Caminho do arquivo gerado
        """
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)

        caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)

        try:
            total_processados = 0
            com_preco = 0
            sem_preco = 0

            with open(caminho_arquivo, 'w', encoding='latin-1') as f:

                for produto in dados:
                    total_processados += 1

                    # Extrai EAN e Descrição
                    ean = str(produto.get('Código EAN', '')).strip()
                    descricao = str(produto.get('Nome Produto', '')).strip()[:30]

                    # Valida se tem código EAN
                    if not ean or ean == '':
                        continue

                    # Calcula preços
                    preco_unitario, qtd, preco_total, tipo_preco = self.calcular_preco_com_quantidade(produto)

                    # Obtém o valor da promoção real
                    valor_promocao = produto.get('Valor Promoção') or 0.0

                    # Formata números com vírgula (padrão PT-BR)
                    preco_total_fmt = f"{preco_total:.2f}".replace('.', ',')
                    preco_promocao_fmt = f"{valor_promocao:.2f}".replace('.', ',')

                    # Contabiliza
                    if preco_total > 0:
                        com_preco += 1
                    else:
                        sem_preco += 1

                    # Monta a linha EXATAMENTE conforme solicitado
                    # Formato: EAN|DESCRICAO|PRECO_TOTAL|PRECO_PROMOCAO\r
                    linha = f"{ean}|{descricao}|{preco_total_fmt}|{preco_promocao_fmt}\r"

                    f.write(linha)

            print(f"\n{'='*70}")
            print(f"✓ Arquivo gerado: {caminho_arquivo}")
            print(f"\n📊 Estatísticas:")
            print(f"   Total de produtos processados: {total_processados}")
            print(f"   Produtos com preço: {com_preco}")
            print(f"   Produtos sem preço: {sem_preco}")
            print(f"{'='*70}")

            return caminho_arquivo

        except Exception as e:
            print(f"✗ Erro ao gerar arquivo output.txt: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def processar_arquivo_gertec(self):
        """
        Processa todos os dados e gera o arquivo output.txt
        Solicita os parâmetros da política comercial

        Returns:
            dict: Estatísticas do processamento
        """
        print("\n" + "="*70)
        print("GERAÇÃO DE ARQUIVO PARA TERMINAL GERTEC 506E - BUSCA PREÇO")
        print("="*70)

        # Solicita os parâmetros
        print("\n📋 Informe os parâmetros da Política Comercial:")
        try:
            id_polcom = int(input("ID da Política Comercial [432]: ").strip() or "432")
            cod_cli = int(input("Código do Cliente [164]: ").strip() or "164")
            cod_estab = int(input("Código do Estabelecimento [0]: ").strip() or "0")
        except ValueError:
            print("✗ Valores inválidos! Usando valores padrão.")
            id_polcom = 432
            cod_cli = 164
            cod_estab = 0

        inicio = datetime.now()

        # Busca os dados
        dados = self.buscar_produtos_com_precos(id_polcom, cod_cli, cod_estab)

        if not dados:
            print("⚠ Nenhum produto encontrado!")
            return None

        # Gera o arquivo output.txt
        print("\n📄 Gerando arquivo output.txt...")
        arquivo_gerado = self.gerar_arquivo_output_pipe(dados)

        fim = datetime.now()
        tempo_decorrido = (fim - inicio).total_seconds()

        estatisticas = {
            'total_produtos': len(dados),
            'arquivo_gerado': arquivo_gerado,
            'tempo_segundos': tempo_decorrido,
            'id_polcom': id_polcom,
            'cod_cli': cod_cli,
            'cod_estab': cod_estab
        }

        print(f"\n⏱ Tempo total de processamento: {tempo_decorrido:.2f} segundos\n")

        return estatisticas

    def fechar_conexao(self):
        """
        Fecha a conexão com o banco de dados
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class ConfiguracaoBanco:
    """
    Gerencia as configurações de conexão ao banco de dados
    """

    ARQUIVO_CONFIG = 'config_banco.json'

    @staticmethod
    def carregar_configuracao():
        """
        Carrega a configuração salva do arquivo

        Returns:
            dict: Configurações do banco ou None se não existir
        """
        if os.path.exists(ConfiguracaoBanco.ARQUIVO_CONFIG):
            try:
                with open(ConfiguracaoBanco.ARQUIVO_CONFIG, 'r') as f:
                    return json.load(f)
            except:
                return None
        return None

    @staticmethod
    def salvar_configuracao(config):
        """
        Salva a configuração no arquivo

        Args:
            config: Dicionário com as configurações
        """
        with open(ConfiguracaoBanco.ARQUIVO_CONFIG, 'w') as f:
            json.dump(config, f, indent=4)
        print(f"✓ Configurações salvas em {ConfiguracaoBanco.ARQUIVO_CONFIG}")

    @staticmethod
    def solicitar_dados_conexao():
        """
        Solicita os dados de conexão ao usuário

        Returns:
            dict: Dicionário com os dados de conexão
        """
        print("\n" + "="*70)
        print("CONFIGURAÇÃO DE CONEXÃO AO BANCO DE DADOS")
        print("="*70)

        servidor = input("Servidor SQL: ").strip()
        banco = input("Nome do Banco: ").strip()

        print("\nTipo de autenticação:")
        print("1 - Windows (Autenticação Integrada)")
        print("2 - SQL Server (Usuário e Senha)")
        tipo_auth = input("Escolha (1 ou 2): ").strip()

        config = {
            'servidor': servidor,
            'banco': banco,
            'tipo_auth': tipo_auth
        }

        if tipo_auth == '2':
            usuario = input("Usuário: ").strip()
            senha = input("Senha: ").strip()
            config['usuario'] = usuario
            config['senha'] = senha

        return config

    @staticmethod
    def criar_connection_string(config):
        """
        Cria a string de conexão a partir da configuração

        Args:
            config: Dicionário com as configurações

        Returns:
            str: String de conexão
        """
        if config['tipo_auth'] == '1':
            return f"DRIVER={{SQL Server}};SERVER={config['servidor']};DATABASE={config['banco']};Trusted_Connection=yes;"
        else:
            return f"DRIVER={{SQL Server}};SERVER={config['servidor']};DATABASE={config['banco']};UID={config['usuario']};PWD={config['senha']}"


def main():
    """
    Função principal que gerencia o fluxo do programa
    """
    gerador = None

    try:
        # Carrega ou solicita configuração
        config = ConfiguracaoBanco.carregar_configuracao()

        if config is None:
            print("\n⚠ Primeira execução detectada!")
            config = ConfiguracaoBanco.solicitar_dados_conexao()

            # Testa a conexão
            print("\nTestando conexão...")
            connection_string = ConfiguracaoBanco.criar_connection_string(config)

            try:
                teste_conn = pyodbc.connect(connection_string)
                teste_conn.close()
                print("✓ Conexão testada com sucesso!")
                ConfiguracaoBanco.salvar_configuracao(config)
            except Exception as e:
                print(f"\n✗ Erro ao testar conexão: {str(e)}")
                print("\nVerifique os dados informados e tente novamente.")
                return
        else:
            print("✓ Configuração carregada do arquivo")
            connection_string = ConfiguracaoBanco.criar_connection_string(config)

        # Cria o gerador e processa
        gerador = GeradorArquivoGertec506E(connection_string)
        gerador.processar_arquivo_gertec()

        print("\n✅ Processo concluído com sucesso!")
        print("📁 Arquivo pronto para uso:")
        print("   • output.txt - Arquivo para Terminal Gertec 506E")

    except KeyboardInterrupt:
        print("\n\n⚠ Operação cancelada pelo usuário")
    except Exception as e:
        print(f"\n✗ Erro fatal: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if gerador is not None:
            gerador.fechar_conexao()
            print("\n✓ Conexão fechada")


if __name__ == "__main__":
    main()