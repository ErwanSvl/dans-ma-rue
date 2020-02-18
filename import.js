const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const {
    Client
} = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    // Create Elasticsearch client
    const client = new Client({
        node: config.get('elasticsearch.uri')
    });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 

    // Création de l'indice
    client.indices.create({
        index: 'incidents',
        body: {
            mappings : {
                properties : {
                    location : { "type" : "geo_point" }
                }
            }
        }
    }, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let incidents = []

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
                incidents.push({
                    "timestamp": data.DATEDECL,
                    "object_id": data.OBJECTID,
                    "annee_declaration": data["ANNEE DECLARATION"],
                    "mois_declaration": data["MOIS DECLARATION"],
                    "type": data.TYPE,
                    "sous_type": data.SOUSTYPE,
                    "code_postal": data.CODE_POSTAL,
                    "ville": data.VILLE,
                    "arrondissement": data.ARRONDISSEMENT,
                    "prefixe": data.PREFIXE,
                    "intervenant": data.INTERVENANT,
                    "conseil_de_quartier": data["CONSEIL DE QUARTIER"],
                    "location": data.geo_point_2d
                })
                /*client.bulk({
                    index: indexName,
                    body: incidents
                }).then(response => {
                    
                })*/
            if (incidents.length / 20000 >= 1) {
                client.bulk(createBulkInsertQuery(incidents), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} incidents`);
                    client.close();
                    console.log('Terminated!');
                });
                incidents = [];
            }
        })
        .on('end', () => {
            client.bulk(createBulkInsertQuery(incidents), (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} incidents`);
                client.close();
                console.log('Terminated!');
            });
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(incidents) {
    const body = incidents.reduce((acc, incident) => {
        //console.log(incident)
      const { timestamp, 
        annee_declaration, 
        mois_declaration, 
        type,
        sous_type,
        code_postal,
        ville,
        arrondissement,
        prefixe,
        intervenant,
        conseil_de_quartier,
        location
    } = incident;
      acc.push({ index: { _index: 'incidents', _type: '_doc', _id: incident.object_id } })
      acc.push({ timestamp, 
        annee_declaration, 
        mois_declaration, 
        type,
        sous_type,
        code_postal,
        ville,
        arrondissement,
        prefixe,
        intervenant,
        conseil_de_quartier,
        location })
      return acc
    }, []);
  
    return { body };
  }

run().catch(console.error);