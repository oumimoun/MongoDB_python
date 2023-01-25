from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
import time
import json
import pymongo


#connexion
connexion = MongoClient()
print(connexion)


#base de donnée à créer s'appelle 'bdcommerce'
bd = connexion["bdcommerce"]

#collection à créer s'appelle 'client'
client = bd["client"]

#autre collection
produit = bd["produit"]

#nouveaux documents (de nouveaux clients)

c1 = {"idcli":1,"nom":"karimi","prenom":"farid"}
c2 = {"idcli":1,"nom":"karimi","prenom":"farid","commandes":[{"idcmde":1,"datecmde": datetime(2000,5,5,10,5,6)},{"idcmde":2,"datecmde": datetime(1980,5,5,10,5,6)}],"adresse":{"pays":"maroc","ville":"rabat"}}
client.insert_one(c1)
client.insert_one(c2)

c= {"idcli":10,"nom":"karimi","prenom":"farid","heureDerniereCommande":time.time()}
client.insert_one(c)


#liste des clients
print("liste des clients :")
listeClients = client.find()
for cli in listeClients[:]:
        print(cli)


#heures des dernières commandes
print("heures des dernières commandes :")
listeClients = client.find()
for cli in listeClients[:]:                             
        if "heureDerniereCommande" in cli.keys():       #cli est un dictionnaire
                print(time.ctime(cli["heureDerniereCommande"])) #ctime() : convertir heure en seconde dans une chaine locale

#ref => https://www.mongodb.com/docs/manual/reference/operator/
#ref (selection & projection) => https://www.analyticsvidhya.com/blog/2020/08/query-a-mongodb-database-using-pymongo/
#selection => ex : clients ayant le nom="karimi" et prenom="farid"
print('clients ayant le nom="karimi" et prenom="farid" :')
selection = {"nom":"karimi","prenom":"farid"}
listeClients_selection = client.find(selection)
for cli in listeClients_selection[:]:
        print(cli)


#operateurs
print("selection 1 :")
selection = {"nom": { "$eq" : "sadiki"}}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

print("selection 2 :")
#$ne : not equal => famille : ($ne,$eq(equal),$gt(greater than),
# $gte(greater than equal),$lt(lesss than),$lte(less than equal))
#$nin : not in => famille : ($nin, $in(in)) 
#$exits : if 'true' => document contains the field (including the case where its value is null)
#$exists => famille(element operators) : ($type : format => { field: { $type: <BSON type> } },
# list of types => ref : https://www.mongodb.com/docs/manual/reference/operator/query/type/) 
#$and => famille : ($and, $or, $nor, $not)
selection = {"$and":[{"nom":{"$ne":"sadiki"}},{"age":{"$exists":True, "$gte":30, "$nin":[30,33]}}]}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

selection = {"$and"["prix":{"$type":"float"},{"gte":50}]}
selection = {"$and"["cdmde":{"idcde":{"$eq":2000}} , {"datecmde":{"$in":[20,23] }}]}
print("selection 3 :")
selection = {"$or":[{"$and":[{"prix":{"$type":"int"}},{"prix":{"$gte":100}}]} , {"qtestock":{"$exists":True}}]}
liste = produit.find(selection)

# selection = {"client":{"nom":{"$exists":True}},"prenom":{"$exist":True}}
# selection = {"age":{"$gte":18}}

for prod in liste[:]:
        print(prod)

print("selection 4 (regex) :")
selection = {"telephone":{"$regex":"^06(\d){8}$"}} #metacharacters => ref : https://www.w3schools.com/python/python_regex.asp
selection1 = {"telephone":{"$regex":"^07(\d){8}$"}}
liste = client.find(selection, selection1)
for cli in liste[:]:
        print(cli)

#requêtes sur les tableaux et les objets
print("selection 5 : clients ayant 2 commandes :")
selection = {"commandes":{"$size":2}}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

print("selection 6 : clients ayant 'Meknes' comme une ville de livraison :")
selection = {"villeslivraison":{"$eq":"meknes"}}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

print("selection 7 : clients ayant 'meknes' et 'rabat' comme des villes de livraison :")
selection = {"villeslivraison":{"$elemMatch":{"$eq":"meknes","$eq":"rabat"}}}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

print("selection 8 : clients ayant 'meknes' ou 'rabat' comme des villes de livraison :")
selection = {"$or":[{"villeslivraison":"meknes"},{"villeslivraison":"rabat"}]}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

print("selection 9 : clients ayant 'marrakech' comme deuxième ville de livraison :")
selection = {"villeslivraison.1":{"$eq":"marrakech"}}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)

print("selection 10 : clients ayant la quantité de la première commande >= 100 :")
selection = {"commandes.0.qte":{"$gte":100}}
liste = client.find(selection)
for cli in liste[:]:
        print(cli)



#projection => ex : extraire l'idcli et le nom
print("extraire l'idcli et le nom (_id aussi extrait) :")
liste = client.find({},{"nom":1,"idcli":1}) #positionner les clés à extraire à 1
for cli in liste[:]:
        print(cli)

print("extraire l'idcli et le nom (_id exclu):")
liste = client.find({},{"nom":1,"idcli":1, "_id":0}) #positionner les clés à extraire à 1
for cli in liste[:]:
        print(cli)

print("exclure l'idcli et le nom :")
liste = client.find({},{"nom":0,"idcli":0}) #on positionne les clés à exclure à 0
for cli in liste[:]:
        print(cli)

#opérations arithmétiques
col = bd["col"]
col.insert_one({"x":-80,"y":3,"dat":datetime(2000,4,7)})
print("col:")
selection = col.find({})
for doc in selection[:]:
        print("x:",doc["x"],"y:",doc["y"],"dat:",doc["dat"].strftime("%d/%m/%Y"))

selection = col.aggregate(
        [
                {"$project":{
                        "x":1,
                        "y":1,
                        "somme":{"$add":["$x","$y"]},
                        "soustraction":{"$subtract":["$x","$y"]},
                        "produit":{"$multiply":["$x","$y"]},
                        "division":{"$divide":["$x","$y"]},
                        "modulo":{"$mod":["$x","$y"]},
                        "_id":1
                        }
                }
        ]
)

for doc in selection:
        print("x:",doc["x"],"y:",doc["y"],"somme:",doc["somme"],
                "soustraction:",doc["soustraction"],"produit:",doc["produit"],"division:",
                doc["division"],"modulo:",doc["modulo"],doc["_id"])

#condition
selection = col.aggregate(
        [
                {
                        "$project":{
                                "x":1,
                                "absX":{"$cond":{"if":{"$gte":["$x",0]},"then":"$x","else":{"$multiply":[-1,"$x"]}}},
                                "sommeInR":{"$cond":{"if":{"$lte":["$y",0]},"then":[{"$multiply":["$y",-1]},{"$add":["$x","$y"]}],
                                "else":{"$add":["$x","$y"]}}},
                                "_id":0
                        }
                }
        ]
)

print("valeurs absolues de x:")
for X in selection:
        print("x : ",X["x"],", |x| : ",X["absX"],X["sommeInR"])

#let
print("extraire l'idcli et la quantité de la première commande de chaque client:")
liste = client.aggregate(
        [
        {"$project": {
        "_id":0,
        "idcli":1,
        "qtepremierecommande": {
          "$let": {
            "vars": {
              "firstMember": {
                "$arrayElemAt": [     #extraire l'élément d'indice 0 au tableau 'commandes'
                  "$commandes",
                  0
                ]
              }
            },
            "in": "$$firstMember.qte"   #dans ce cas '$$firstMember' représente le premier élément du tanleau 'commandes'
          }
        }
      }
    }
]
)
for cli in liste:
        print(cli)

#a revoir
#valeurs commandés par un client => somme(prixu*qte)(prixu et qte concernent les produits des commandes du client en question)
# liste = client.aggregate(
#         [
#                 {
#                         "$project":{
#                                 "produitqp":{
#                                         "$let":{
#                                                 "vars":{
#                                                         "prodqp":{"$multiply":["$qte","$prixu"]}
#                                                 },
#                                                 "in":{}
#                                         }
#                                 }
#                         }
#                 }
#                 {
#                         "$group":
#                         {
#                         "_id":"$idcli",
#                         "valeur":{"$sum":"$multiply":["$qte","$prixu"]}
#                         }
                        
#                 }
#         ]
# )

liste = client.aggregate(
        [
                {
                        "$project":
                        {
                                "qte":
                                {
                                        "$let":
                                        {
                                                "vars":{
                                                        "qteprixu":{
                                                                "$arrayElemAt":["$commandes"]
                                                        }
                                                }
                                        }
                                }
                        }
                }
        ]
)
selection = client.aggregate(
        [
        
                {"$project":{"idcli":1}},{"$group":{"_id":"idcli","sommeqte":{"$sum":"$"}}}
        ]
)

#comment exprimer des jointures
bdformation = connexion["bdformation"]
stagiaire = bdformation["stagiaire"]
filiere = bdformation["filiere"]
stagiaire.insert_many([
        {"cne":"100000","nom":"karimi","prenom":"farid","filiere":1},
        {"cne":"100001","nom":"sadiki","prenom":"farid","filiere":2},
        {"cne":"100002","nom":"karimi","prenom":"halima","filiere":1},
        {"cne":"100003","nom":"chakiri","prenom":"amina"},
])

stagiaire.insert_many([
        {"cne":"100010","nom":"karimi","prenom":"farid","filiere":1},
        {"cne":"100021","nom":"sadiki","prenom":"farid","filiere":2},
        {"cne":"100032","nom":"karimi","prenom":"halima","filiere":1},
        {"cne":"100043","nom":"chakiri","prenom":"amina"},
])

filiere.insert_many([
        {"numero":1,"designation":"Développement Digital"},
        {"numero":2,"designation":"Infrastructure Digitale"},
        {"numero":3,"designation":"Gestion des entreprises"},
])
#jointure externe à gauche
selection = stagiaire.aggregate([
                        {
                        "$lookup":
                                {
                                "from":"filiere",
                                "localField":"filiere",
                                "foreignField":"numero",
                                "as":"filieres"
                                }
                        }
        ])
print("stagiaires avec filières:")
for s in selection:
        print(s)
#jointure interne
selection = stagiaire.aggregate([
                        {
                                "$match":{"filiere":{"$exists":True}
                                }
                        },
                        {
                        "$lookup":
                                {
                                "from":"filiere",
                                "localField":"filiere",
                                "foreignField":"numero",
                                "as":"filieres"
                                }
                        }
        ])
print("stagiaires avec filières:")
for s in selection:
        print(s)

#findOne() => retourne le seul document qui vérifie une une condition (le premier si plusieurs documents vérifient la condition)
selection = {"age":{"$gte":20}}
cli = client.find_one(selection)
print("infos du seul (ou premier client) ayant l'age >= 15 :")
if cli is not None:
       print(cli)   #{'_id': ObjectId('638c8f2085b07a35ca62d4e7'), 'idcli': 10, 'nom': 'fatihi', 'prenom': 'farid', 'heureDerniereCommande': 1670156064.6155472, 'age': 25}
#avec projection
selection = {"age":{"$gte":20}}
cli = client.find_one(selection,{"nom":1,"prenom":1,"age":1,"_id":0}) #en principe, tous les champs sont extraits sauf ceux exclus
print("infos du seul (ou premier client) ayant l'age >= 15 => nom,prenom, et age :")
if cli is not None:
       print(cli)   #{'nom': 'fatihi', 'prenom': 'farid', 'age': 25}

#aggregation pipline stages
groupes = client.aggregate(
        [
                #stage1
                {"$match":{"nom":{"$eq":"karimi"}}}
                #stage2
                ,
                {"$group":{"_id":"clegroupe", "age_minimal":{"$min":"age"}}}
        ]
)
clients_sadiki = client.aggregate([{"$match":{"nom":{"$eq":"sadiki"}}}])

print("clients_sadiki : ")
for cli in clients_sadiki:
        print(cli)

accumulateur1 = client.aggregate(
        [
                {"$match":{"age":{"$exists":True}}},
                {"$group":{"_id":"$nom","age_min":{"$min":"$age"}}}
        ]
)
print("accumulateur 1")
for e in accumulateur1:
        print(e)
"""affichage:
{'_id': 'karimi', 'age_min': 17}
{'_id': 'sadiki', 'age_min': 15}
{'_id': 'fatihi', 'age_min': 25}"""

accumulateur2 = client.aggregate(
        [
                {"$match":{"age":{"$exists":True}}},
                {
                "$group":
                        {"_id":{"nom":"$nom","prenom":"$prenom"},
                        "age_min":{"$min":"$age"}, #liste d'opérateurs d'accumulation : https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/
                        "age_max":{"$max":"$age"},
                        "age_sum":{"$sum":"$age"},
                        "age_avg":{"$avg":"$age"}, 
                        "age_count":{"$count":{}} #count ne prend pas d'arguments
                }
                }
        ]
) 
print("accumulateur 2")
for e in accumulateur2:
        print(e)

accumulateur3 = client.aggregate(
        [
                {
                        "$group":
                        {
                        "_id":{"nom":"$nom","prenom":"$prenom"},
                        "age_min":{"$min":"$age"}
                        }
                },
                {
                        "$sort":{"_id.nom":1, "_id.prenom":-1}
                }
        ]
)

print("tri groupe :")
for e in accumulateur3:
        print(e)



#Distinct()
noms_distinct = client.distinct("nom")
print("noms distincts :")
for n in noms_distinct[:]:
        print(n)

nomsquery_distinct = client.distinct("nom",{"age":{"$exists":True}})
print("noms distincts avec requête :")
for nq in nomsquery_distinct[:]:
        print(nq)

nomsprenoms_distinct = client.aggregate(
        [
                {
                "$group": {"_id":{"nom":"$nom","prenom":"$prenom"}}
                }
        ]
)
print("noms,prenoms distincts :")
for np in nomsprenoms_distinct:
        print(np)


#Tri
tri = client.aggregate(
        [
        {"$project":{"nom":1,"prenom":1, "_id":0}}, #seuls les champs 'nom' et 'prenom' sont affichés
        {"$sort":{"nom":1, "prenom":-1}} #tri croissant par nom (1), PUIS décroissant par prenom (-1)
        ]
)
print("tri ")
for e in tri:
        print(e)

groupaccumulateurtri1 = client.aggregate(
        [
                {
                        "$group":{"_id":"$nom","idpremierclient":{"$first":"$idcli"}}
                },
                {"$sort":{"idcli":-1}}
        ]
)
print("premier client de chaque groupe")
for cli in groupaccumulateurtri1:
        print(cli)


#Requêtes de modifications
#mise à jour
maj1 = client.update_one(
        {}, #requête(selection)
        {"$set":{"nom":"nouvel nom","prenom":"nouvel prenom"},"$inc":{"age":1}} #$set => modifier le champ, $inc => incrementer(|decrementer (valeur negative)) le champ
)
print("maj1 nombre de documents vérifiant la condition : ", maj1.matched_count) #1 => premier document
print("maj1 nombre de documents modifiés : ", maj1.modified_count) #1 => premier document

maj2 = client.update_many(
        {"nom":{"$eq":"sadiki"}}, 
        {"$inc":{"age":1}}, 
        upsert=True             #si le champ 'age' n'existe pas, on l'ajoute (on lui affecte la valeur 1) 
)
print("maj2 nombre de documents vérifiant la condition : ", maj2.matched_count) #5
print("maj2 nombre de documents modifiés : ", maj2.modified_count) #5

#modifier la deuxième ville de livraison en 'Azrou'
maj3 = client.update_many(
        {"nom":"karimi", "commandes":{""}},
        {"$elemMatch":{"qte":{"$gt":100}}}
)
#a revoir
#ajouter une ville de livraison
#ajouter le champ 'nationalité' au client ayant l'idcli = 22
#augmenter de 10% la quantité de la deuxième commande du client ayant l'idcli = 23

#suppression
suppression_unique = client.delete_one({"nom":"karimi"})
print("suppression d'un document unique (ou le premier):")
print("nombre de documents supprimés: ",suppression_unique.deleted_count)

suppression_multiple = client.delete_many({"nom":"karimi"})
print("suppression de plusieurs documents:")
print("nombre de documents supprimés : ",suppression_multiple.deleted_count)

#Création des indexs
#rq : un index unique est créé par defaut sur le champ '_id'
#index ascendant sur le nom de client
res = client.create_index([("nom",1)]) #nom par defaut : nomchamp_(1|-1)
print("1 index créé avec succès sous le nom : ", res)
#index descendant sur le prenom de client
client.create_index([("prenom",-1)])
print("2 index créé avec succès")
#index composé (nom et prenom)
client.create_index([("nom",1),("prenom",-1)]) #nom par defaut : nom_1_prenom_-1
print("3 index créé avec succès")
#index unique
client.create_index([("idcli",pymongo.DESCENDING)],unique=True)
print("4 index créé avec succès")
#changer le nom par defaut
client.create_index([("idcli",pymongo.ASCENDING)],name="index_idcli_asc") #index appelé 'index_idcli_asc'
print("5 index créé avec succès")

#afficher la liste des index de la collection client
client.index_information()

#suppression des index
#suppression de l'index nommé 'nom_1'
client.drop_index('nom_1')
#suppression de tous les index (sauf celui créé par défaut sur le champ '_id')
client.drop_indexes()



#Import / export des données (mongoexport,mongoimport)
"""=> installer les outils (séparés du serveur de bd) : 'MongoDB Command Line Database Tools' 
   => ajouter le chemin d'installation au path comme variable d'environnement"""
#exporter la collection client
#=> mongoexport --host localhost --port 27017 --db bdcommerce --collection client --type json --out C:/Users/younes/Documents/bd_exports_imports/clients.json

#importer les documents dans une collection de la bd
#importer le fichier clients.json dans la nouvelle collection appelée 'nouveauclient'; la commande 'mongoexport' se charge de créer la collection avant de procéder à l'importation 
#=>mongoimport --host localhost --port 27017 --db bdcommerce --collection nouveauclient --type json --out C:/Users/younes/Documents/bd_exports_imports/clients.json

#sauvegarde et restauration
#sauvegarder la bd bdcommerce
#=> mongodump --host localhost --port 27017 --db bdcommerce --out C:/Users/younes/Documents/bd_exports_imports

#importer la bd bdcommerce dans la bd bdcommerce_2(pas encore créée)
#=> mongorestore --host localhost --port 27017 --db bdcommerce_2 C:/Users/younes/Documents/bd_exports_imports/bdcommerce


#Sécurité des accès (authentification )
#gestion des utilisateurs (ref : https://www.mongodb.com/docs/manual/reference/command/nav-user-management/)
connexion = MongoClient(host="localhost",port=27017)
#lister les bd
print("liste des bd:")
for base  in connexion.list_database_names():
        print(base)

bd = connexion["bdcommerce"]
#lister les collections d'une bd
print("liste des collections de la bd bdcommerce:")
for col in bd.list_collection_names():
        print(col) 

#création d'un utilisateur
#nom d'utilisateur:'karimi', mot de passe:'123456', rôle:'dbAdmin'(liste des rôles => https://www.mongodb.com/docs/manual/reference/built-in-roles/)
bd.command("createUser", "karimi", pwd="123456", roles=["dbAdmin"])
#changer le mot de passe
bd.command("updateUser","karimi",pwd="karimi2022")
print("pwd changé avec succès")
#supprimer un utilisateur
bd.command("dropUser","karimi")
print("utilisateur supprimé avec succès")
#lister les utilisateurs d'une bd
print("liste des utilisateurs de la bd bdcommerce:")
listing = bd.command("usersInfo")
for u in listing['users']:
        print(u)

#création d'un rôle
#pour cibler n'importe quelle bd et le cluster, exécuter la commande sur la bd 'admin'
#ref des actions associées à un rôle => https://www.mongodb.com/docs/manual/reference/privilege-actions/#:~:text=Privilege%20actions%20define%20the%20operations,of%20resources%20and%20permitted%20actions.
bdadmin = MongoClient()['admin']
#bdadmin.command({"createRole":"monRole","privileges":[{ "resource": { "db": "", "collection": "" }, "actions": [ "find"] }],"roles":[]})
print("le role 'monRole' a été créé avec succès")

bdadmin.command({
        "createRole":"role1", 
        "privileges":
        [
                {"resource":{"cluster":True},"actions":["shutdown"]},
                {"resource":{"db":"bdformation","collection":""},"actions":["insert","remove","changePassword"]} ##on ne peut pas spécifier la base de donnée sans spécifier le champ 'collection'
        ],
        "roles":
        [
                 { "role": "dbAdmin", "db": "bdcommerce" },
                  {"role": "dbOwner", "db": "bdISTA" }
        ]
})
print("le role 'role1' a été créé avec succès")

bdformation = MongoClient()['bdformation']
"""bdformation.command({
        "createRole":"rolebdformation", 
        "privileges":
        [
                {"resource":{"db":"bdformation","collection":"stagiaire"},"actions":["insert","remove","update","find"]} 
        ],
        "roles":
        [
                 { "role": "dbAdmin", "db": "bdformation" },
        ]
})"""
print("le role 'rolebdformation' a été créé avec succès")

#lister les rôles
#lister les infos du rôle 'role1' de la bd 'bdadmin'
rolesinfos = bdadmin.command({"rolesInfo":"role1"})
print("infos des rôles de bdadmin : ")
print(rolesinfos)

#modifier un rôle
#pour modifier un rôle, on doit fournir le tableau des privilèges ou le tableau des rôles, ou bien les deux  
bdformation.command({
        "updateRole":"rolebdformation", 
        "privileges":
        [
                {"resource":{"db":"bdformation","collection":"filiere"},"actions":["createIndex","createCollection","dropCollection"]} 
        ],
        "roles":
        [
                 { "role": "readWrite", "db": "bdformation" },
                 { "role": "userAdmin", "db": "bdformation" },
        ]
})
print("le rôle 'rolebdformation' a été modifié avec succès") 
#on peut aussi ajouter ou retirer des rôles et des privilèges à un rôle
#retirer le privilège 'dropCollection' au rôle 'rolebdformation'
bdformation.command({
        "revokePrivilegesFromRole":"rolebdformation",
        "privileges":
        [
                {"resource":{"db":"bdformation","collection":"filiere"},"actions":["dropCollection"]} 
        ]
}
)
print("privilège retiré avec succès")

#retirer le rôle 'userAdmin' au rôle 'rolebdformation'
bdformation.command({
        "revokeRolesFromRole":"rolebdformation",
        "roles":
        [
                 { "role": "userAdmin", "db": "bdformation" },
        ]
}
)
print("rôle retiré avec succès")

#ajouter le privilège 'createUser' au rôle 'rolebdformation'
bdformation.command({
        "grantPrivilegesToRole":"rolebdformation",
        "privileges":
        [
                {"resource":{"db":"bdformation","collection":"filiere"},"actions":["createUser"]} 
        ]
}
)
print("privilège octroyé avec succès")

#ajouter le rôle 'dbAdmin' au rôle 'rolebdformation'
bdformation.command({
        "grantRolesToRole":"rolebdformation",
        "roles":
        [
                 { "role": "dbAdmin", "db": "bdformation" },
        ]
}
)
print("rôle octroyé avec succès")

#changer les rôles d'un utilisateur
bd.command("updateUser","younes",roles=["userAdmin","readWrite"])
print("rôles changés avec succès")


#suppression d'un rôle
#supprimer un rôle par son nom
bdadmin.command({"dropRole":"role1"})
print("rôle supprimé avec succès")

#supprimer tous les rôles d'une bd
bdformation.command({"dropAllRolesFromDatabase":1})
print("TOUS les rôles de la bd ont été supprimés avec succès")

#autres
#supprimer une bd
connexion.drop_database("bdcommerce_2")
print("bd supprimée avec succès")
print("fin")

#autres ref
#liste des privilèges => https://www.mongodb.com/docs/manual/reference/privilege-actions/#:~:text=Privilege%20actions%20define%20the%20operations,of%20resources%20and%20permitted%20actions.

#EXERCICE => Gestion des utilisateurs et rôles avec TESTS
