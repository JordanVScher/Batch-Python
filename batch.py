# -*- coding: utf-8 -*-

#Desafio Back-End
#Sistema Batch com dados em BD
#Jordan Victor Scher - 05/09/2017


import MySQLdb
import codecs
import sys
from datetime import datetime


def criaTabela(conn):
    try:
        cur = conn.cursor()
        sql = """show tables like 'tb_customer_account'"""
        cur.execute(sql)
        if cur.fetchone():
            print("Tabela tb_customer_account já existe.")
        else:
            try:
                sql = """create table tb_customer_account(
                id_customer int not null auto_increment,
                cpf_cnpj varchar(14),
                nm_customer varchar(100),
                is_active bool,
                vl_total numeric,

                primary key(id_customer)
                );"""

                cur.execute(sql)
                print("Tabela tb_customer_account criada com sucesso!")
            except:
                print("Erro ao crir tabela tb_customer_account.")
    except:
        print("Erro ao pesquisar tabela tb_customer_account.")

def gravaDados(conn):

    customer = '' # lista dos dados do 'customer' de cada linha
    sql = ''
    try:
        fo = codecs.open("inputDados.txt", "r", "utf-8")

        try:
            cur = conn.cursor()
            sql = """select * from tb_customer_account"""
            cur.execute(sql)
            if not cur.fetchone():
                print("Inserindo dados. Aguarde.")
                for i in fo:
                    customer = [x.strip() for x in i.split(',')] # remove vírgula

                    sql = """insert into tb_customer_account (`id_customer`, \
                    `cpf_cnpj`, `nm_customer`, `is_active`, `vl_total`) values \
                    (%u, '%s', '%s', %u, '%s');""" % (int(customer[0]), customer[1], customer[2], int(customer[3]), customer[4])
                    cur.execute(sql)
                    conn.commit()

                    # a linha abaixo imprime no console o nome e o id de cada cliente cadastrado
                    # escolheu-se não imprimir esses dados para não prejudicar a visibilidade do resultado
                    # print("Cliente " + customer[2] + " [ID:" + customer[0] + "] inserido com sucesso!")

                fo.close()
            else:
                print("Erro ao gravar dados! Tabela não está vazia!")

        except:
            conn.rollback()
            print("Erro ao gravar dados!")
            print( "Erro: %s" % sys.exc_info()[0] )

    except:
        print("Erro ao abrir arquivo 'input.txt'!")
        print( "Erro: %s" % sys.exc_info()[0] )



def mediaFinal(conn, idMax, idMin, valorMin):

    try:
        cur = conn.cursor()

        sql = """select avg(q.vl_total) from (select vl_total
        from tb_customer_account as t where t.vl_total > %s
        and (t.id_customer > %s and t.id_customer < %s))q;""" % (valorMin, idMin, idMax)

        cur.execute(sql)
        resultado = cur.fetchone()
        print("A média é: " + str(resultado[0]))
        return resultado[0]

    except:
        print("Erro ao calcular média!")
        print( "Erro: %s" % sys.exc_info()[0] )
        return None


def imprimeNomes(conn, idMax, idMin, valorMin):
    # OBS: foi escolhido imprimir os nomes em um arquivo texto para que o /
    # resultado da operação não comprometa a visualização dos resultados das outras operações
    try:
        fo = codecs.open("output.txt", "w+", "utf-8")
        cur = conn.cursor()
        sql = """select nm_customer,vl_total from tb_customer_account as t where t.vl_total > %s
        and (t.id_customer > %s and t.id_customer < %s) order by vl_total desc;""" % (valorMin, idMin, idMax)

        cur.execute(sql)
        resultados = cur.fetchall()
        contador = 0

        #print("Os nomes seguidos de seus valores são:")
        for i in resultados:
            # OBS: foi escolhido imprimir os nomes em um arquivo texto para que a impressão do
            # resultado da operação não comprometa a visualização dos resultados das outras operações
            # print(i[0] + " -> " + str(i[1]))

            fo.write(i[0] + " -> " + str(i[1]) + "\n")
            contador += 1

        fo.write("\n" + str(contador) + " clientes contabilizados.\n")
        fo.write("Horário de execução: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        fo.close()
        print("Os nomes dos " + str(contador) + " clientes contabilizados foram impressos no arquivo output.txt")


    except:
        print("Erro ao salvar nomes")
        print( "Erro: %s" % sys.exc_info()[0] )
        fo.close()


def conectaBanco():
    # OBS: os valores do connect devem ser mudados
    # OBS2: não foi usado um arquivo externo para a senha por uma questão de conveniência
    conn = None
    try:
        conn = MySQLdb.connect(host="localhost",user="root",
                  passwd="senha_vem_aqui",db="nome_do_banco",)
        return conn
    except:
        print( "Erro: %s" % sys.exc_info()[0] )
        return None


if __name__ == "__main__":
    conn = conectaBanco()

    if conn is None:
        print("Ocorreu um erro ao conectar com o banco.")
    else:
        print("Conexão realizada com sucesso.")
        criaTabela(conn)
        gravaDados(conn)

        # Para poder trocar os valores limites da query
        idMax = 2700 # índice de onde parará a busca
        idMin = 1500 # índice de onde começará a busca
        valorMin = 560 # valor mínimo de vl_total

        if mediaFinal(conn, idMax, idMin, valorMin) is not None:
            imprimeNomes(conn, idMax, idMin, valorMin)
        else:
            print("Nenhum cliente se enquadra nas características propostas.")

    print("Programa encerrado.")
