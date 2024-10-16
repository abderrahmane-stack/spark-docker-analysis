from pyspark import SparkConf, SparkContext

appName = "analysis"
conf = SparkConf().setAppName(appName) 
sc = SparkContext(conf=conf)

RDD = sc.textFile("/usr/local/spark/app/arbres.csv")

# 1. Afficher le nombre de lignes
nombre = RDD.count()
print("################")
print("Number of lines: {}".format(nombre)) 

# 2. Calculer et afficher la hauteur moyenne des arbres.
class Arbre(object):
    def __init__(self, ligne):
        # Make sure to use the correct delimiter
        self.champs = ligne.split(";")  # Change to '\t' if needed

    def __str__(self):
        return "Arbre[%s]" % ', '.join(self.champs)

    def genre(self):
        if self.champs[2] == u'GENRE': return None
        return self.champs[2]

    def hauteur(self):
        try:
            return float(self.champs[6])
        except:
            return None

arbres = RDD.map(lambda ligne: Arbre(ligne))
hauteurs = arbres.map(lambda arbre: arbre.hauteur())
hauteurs_ok = hauteurs.filter(None)
moyenne = hauteurs_ok.mean()
print("################")
print("Hauteur moyenne :", moyenne)

# construire des paires (hauteur, genre)
hauteurs_genres = arbres.map(lambda arbre: (arbre.hauteur(), arbre.genre()))

# suppression des paires dont la hauteur ou le genre sont inconnus
hauteurs_genres_ok = hauteurs_genres.filter(lambda hg: hg[0] is not None and hg[1] is not None)
classement = hauteurs_genres_ok.sortByKey(ascending=False)

# affichage de la première paire (= le plus grand arbre)
grand = classement.first()
print("################################")
print("Plus grand arbre :", grand)

genres = arbres.map(lambda arbre: arbre.genre()).filter(None)

# construction de paires (genre, 1)
genres_nombres = genres.map(lambda genre: (genre, 1))

# reduce genre par genre
genres_total = genres_nombres.reduceByKey(lambda a,b: a+b)

# affichage des cumuls par genre
print("#####################################")
print("#####################################",genres_total.collect())
print("##########################################")