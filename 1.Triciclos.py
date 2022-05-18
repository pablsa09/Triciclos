from pyspark import SparkContext
import sys

def elim_dup(lista):
    #Función auxiliar que elimina los duplicados de una lista
    new_list = []
    for elem in lista:
        if elem not in lista:
            new_list.append(elem)
    return new_list

def ord_tupla(tupla):
    #Dada una tupla la función la ordena de menor a mayor
    if tupla[0] < tupla[1]:
        return (tupla[0],tupla[1])
    else:
        return (tupla[1],tupla[0])
        
def to_tupla(arista):
    #Dada una lista en forma de cadena de caracteres, la función la devuelve como tupla ordenada
    arista = arista.strip().split(',')
    #Tomamos los nodos
    (a,b) = ord_tupla(arista)
    return (a,b)
    """
    Hay que destacar que al usar ord_tupla en else puede darse el caso a==b, por lo que el orden
    de cada uno de ellos en la tupla no importa, y en este caso hacemos (b,a)
    """
               
def simplifica(grafo):
    """
    Dado un grafo leído de un archivo de texto, simplifica elimina los bucles y las arista repetidas.
    Suponemos que las aristas del grafo se corresponden con líneas del fichero.
    """
    grafo = grafo.map(to_tupla) #Transformamos cada arista en tupla
    grafo = grafo.filter(lambda x: x[0] != x[1]) #Eliminamos los bucles
    return elim_dup(grafo) #Eliminamos las aristas duplicadas

def lista_adj(arista):
    """
    Dado un nodo en forma de tupla ordenada, esta función devuelve la lista de adyacencias
    como en la pista 2
    """
    adj = []
    for i in range(len(arista[1])):
        adj.append(((arista[0],arista[1][i]),'exists'))
        for j in range(i+1,len(arista[1])):
            (i,j) = ord_ind(arista[1][i],arista[1][j])
            tupla = ord_tupla(arista[1][i],arista[1][j])
            adj.append((tupla,('pending',arista[0])))
    return adj

def triciclos(sc,filename):
    #Dado el archivo de texto, esta función devuelve los triciclos
    grafo =  sc.textFile(filename)
    grafo_s = simplifica(rdd)
    adjs = grafo_s.map(lista_adj) #Tomamos la lista de listas de adyacencia
    triciclos = []
    for adj in adjs: #Buscamos los triciclos
        new_adj = adj.filter(lambda x: x[1] >=2) #Filtramos los triciclos
        new_adj = new_adj.filter(lambda x: 'exists' in x[1])
        triciclos.append(new_adj)
    return triciclos
  

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            principal(sc,sys.argv[1])
           
            
            
            
            
