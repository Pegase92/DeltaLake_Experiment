from pyspark import SparkContext
sc = SparkContext(master="local", appName="First App")

def TakeOneField(Vecteur):
    print(Vecteur[7])
    return(Vecteur[7])

def Creation_RDD_and_Save():

    MyRDD = sc.textFile("/home/benoit/DeltaLake_Experiment/Data/MyData.csv")
    #======> Split each line by ';' and retrieve record where zip code begin by 60
    MyRecordRDD = MyRDD.map(lambda x: x.split(";")).filter(lambda x: x[6].find("60")==0)
    #======> Effectue un tri sur la 7ème colonne (commune) et sur le filtrage précédent
    print(MyRecordRDD.sortBy(lambda x:x[7]).take(10))
    #======> Sauve le RDD sous forme d'un fichie 'texte'
    MyRecordRDD.sortBy(lambda x:x[7]).saveAsTextFile("/home/benoit/DeltaLake_Experiment/Data/File2.txt")
    #======> Effectue un tri complet du fichier par le nom de la commune et le sauvegarde
    MyRecordRDD1 = MyRDD.map(lambda x: x.split(";")).sortBy(lambda x:x[7])
    MyRecordRDD1.saveAsTextFile("/home/benoit/DeltaLake_Experiment/Data/File3.txt")

    return(MyRDD)

def Lecture_File_RDD():

    MyRDD = sc.textFile("/home/benoit/DeltaLake_Experiment/Data/MyData.csv")
    MyRDD0 = MyRDD.map(lambda x:x.split(";")).map(lambda x:x[0])
    MyRDD0.saveAsTextFile("/home/benoit/DeltaLake_Experiment/Data/File4.txt")
    MyRDD3 = sc.textFile("/home/benoit/DeltaLake_Experiment/Data/File4.txt")
    MyRDD1 = MyRDD.map(lambda x:x.split(";")).map(lambda x:x[6])
    MyRDD5 = MyRDD3.map(lambda x:x.split(";")).map(lambda x:x[0])
    print(MyRDD1.take(5))
    print(MyRDD5.take(5))

Lecture_File_RDD()




