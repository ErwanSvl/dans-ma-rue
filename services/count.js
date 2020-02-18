const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    client
        .count({
            index: 'incidents',
            body: {
                query: {
                    range: {
                        timestamp: {
                            gte: from,
                            lt: to
                        }

                    }
                }
            }
        })
        .then(resp => console.log(callback({
            count: resp.body.count
        })));

}

exports.countAround = (client, lat, lon, radius, callback) => {
    client
        .count({
            index: 'incidents',
            body: {
                query: {
                    "bool": {
                        "filter": {
                            "geo_distance": {
                                "distance": radius,
                                "location": [lon, lat]
                            }
                        }
                    }
                }
            }
        })
        .then(resp => callback({
            count: resp.body.count
        }), err => console.error(err.meta.body.error));
}