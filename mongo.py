from pymongo import MongoClient, ASCENDING
import insert_books
import insert_authors

try: 
    client = MongoClient("mongodb://localhost:27017/")
    print("Successful connection to MongoDB.\n") 
except:
    print("Could not connect to MongoDB.") 

database = client['library']

books = database.books
authors = database.authors

books.drop()
authors.drop()

def insertDataToMongo():
    books.insert_many([insert_books.book1, insert_books.book2, insert_books.book3, 
                    insert_books.book4, insert_books.book5])
    authors.insert_many([insert_authors.auth1, insert_authors.auth2])
    print("Data inserted!")
    
def displayAllCollections():
    print("Displaying inserted collections:")
    print(database.list_collection_names())
    print("")
    

def displayEverythingAboutBooks():
    print ("\nDisplaying everything about the books in the 'books' (sorted by title):")
    for i in books.find().sort("Title", ASCENDING):
        print(i)
    print("\n")
        
def listEmbededProperties():
    print("\n")
    for i in books.find({},{"Review", "Publisher"}):
        bookTitle = books.find_one({"_id":i["_id"]},{"Title"})
        print(f"Book Title: {bookTitle['Title']}")
        print(f"Review: {i['Review']}\nPublisher: {i['Publisher']}")
    print("\n")

def countStarsWithAggregation(authorID):
    agr = [{'$match': {'AuthorID': authorID}},
           {'$group': {'_id': "$AuthorID", 'total': {'$sum': "$Review.Stars"} }}]
    result = list(database.books.aggregate(agr))
    print('\nAggregation Example: books that were written by author {0} have earned {1} stars.'.format(result[0]['_id'], result[0]['total']))
    print("")

def countStarsWithMap_Reduce(authorID):
    mapF = """
        function() {{
        if (this.AuthorID == '""" + authorID + """')
            emit(this.AuthorID, this.Review.Stars);
    }};
    """

    reduceF = """
        function(AuthorID, Pages){
            return Array.sum(Pages);
    };
    """

    result = database.command(
        'mapReduce',
        'books',
        map = mapF,
        reduce = reduceF,
        out = {'inline': 1}
    )
    print("")
    print(result)
    print("")

def main():
    insertDataToMongo()
    displayAllCollections()
    authors = ["auth1", "auth2"]
    while (True):
        print("Select action:\n1 - Display everything about books")
        print("2 - List embeded properties")
        print("3 - Count stars w/ aggregation")
        print("4 - Count stars w/ map_reduce")
        print("5 - Quit")
        option = input()
        if option == "1":
            displayEverythingAboutBooks()
        elif option == "2":
            listEmbededProperties()
        elif option == "3":
            for i in authors:
                countStarsWithAggregation(i)
        elif option == "4":
            for i in authors:
                countStarsWithMap_Reduce(i)
        elif option == "5":
            print("Closing...")
            break
        else:
            print("Enter valid number")
        
if __name__ == "__main__":
    main()