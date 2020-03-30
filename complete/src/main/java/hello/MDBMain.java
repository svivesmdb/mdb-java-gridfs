package hello;

import com.mongodb.Block;
// Connectivity to MongoDB
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

// BSON-related imports
import org.bson.Document;
import org.bson.types.ObjectId;


// GridFS - related imports
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.model.*;


import java.io.*;
import java.util.Arrays;

public class MDBMain {
	
	public static void findAllExpedients(MongoCollection<Document> col, String field, String equalsto) {
		

		Block<Document> printBlock = new Block<Document>() {
		       @Override
		       public void apply(final Document document) {
		           System.out.println(document.toJson());
		       }
		};
		
		Document filter = new Document(field, new Document("$eq", equalsto));
		
		col.find(filter).forEach(printBlock);
		
	}
	
	
	public static ObjectId pushFile(Document doc, GridFSBucket gridFSFilesBucket, String src, String destfilename) {
		try {
		    InputStream streamToUploadFrom = new FileInputStream(new File(src));
		    // Create some custom options
		    GridFSUploadOptions options = new GridFSUploadOptions()
		                                        //.chunkSizeBytes(358400)
		                                        .metadata(doc);

		    return gridFSFilesBucket.uploadFromStream(destfilename, streamToUploadFrom, options);
		    
		} catch (FileNotFoundException e){
		   // handle exception
		}
		
		return null;
	}
	
	
	public static void main(String[] args) {
		
		// Obrir el client
		 MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

		 // Obrir la base de dades de justicia
		 MongoDatabase database = mongoClient.getDatabase("justicia");
		 
		 // Referencies a les coleccions de arxius
		 GridFSBucket gridFSFilesBucket = GridFSBuckets.create(database, "justicia_arxius");		
		 
		 //
		 // Forma 2: Recomanada, metadades per una banda i arxius de GridFS per altra, referenciats.
		 //			Permet evolucionar a futur ja que si s'esborren de GridFS els arxius encara tenim les metadades
		 //
		 
		 // Podem insertar un document i referenciar l'arxiu insertat
		 ObjectId id_of_inserted_doc = pushFile(new Document(), gridFSFilesBucket, "/Users/sergivives/Documents/1-Clients/Partners/T-Systems/8M.png", "8M-Referenciat");
		 
		 // Crear el document i insertar amb el valor del arxiu referenciat
		 Document doc_independent = 
				 new Document("name", "Arxiu de expedient 8M-referenciat")
			        .append("referencia_arxiu", id_of_inserted_doc)
			        .append("tipus", "pdf per justicia")		        
			        .append("pertany_a", Arrays.asList("Jurisdiccio 17", "Jurisdiccio 16"))
			        .append("informacio_adicional", new Document("Jutge", "Joan Esteve").append("Num Oficina", 502).append("localitzacio",109));
		

		 MongoCollection<Document> col_metadades = database.getCollection("justicia_arxius.metadades");
	 
		 col_metadades.insertOne(doc_independent);
			 		 
		 findAllExpedients(col_metadades, "pertany_a", "Jurisdiccio 17");
		 
		 
	}
}
