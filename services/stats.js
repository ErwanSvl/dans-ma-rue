const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client
        .search({
            index: 'incidents',
            body: {
                aggs: {
                    "arrondissements": {
                        "terms": {
                            "field": "arrondissement.keyword",
                            size: 20
                        }
                    }
                }

            }
        })
        .then(resp => callback(formatStatArrondissement(resp.body.aggregations.arrondissements.buckets)), err => console.error(err.meta.body.error));

}
//mets en forme le résultat comme demandé dans l'exercice
function formatStatArrondissement(data) {
    let acc = [];
    for (let index = 0; index < data.length; index++) {
        const element = data[index];
        acc.push({
            arrondissements: element.key,
            count: element.doc_count
        })
    }
    return acc;
}

exports.statsByType = (client, callback) => {
    client
        .search({
            index: 'incidents',
            body: {
                aggs: {
                    "types": {
                        "terms": {
                            "field": "type.keyword",
                            size: 5
                        },
                        aggs: {
                            "sous_types": {
                                "terms": {
                                    "field": "sous_type.keyword",
                                    size: 5
                                }
                            }
                        }
                    }
                }

            }
        })
        .then(resp => callback(formatStatType(resp.body.aggregations.types.buckets)), err => console.error(err.meta.body.error));
}

//mets en forme le résultat comme demandé dans l'exercice
function formatStatType(data) {
    let acc = [];
    for (let index = 0; index < data.length; index++) {
        const element = data[index];
        let subAcc = [];
        for (let subIndex = 0; subIndex < element.sous_types.buckets.length; subIndex++) {
            const subElement = element.sous_types.buckets[subIndex];
            subAcc.push({
                sous_type: subElement.key,
                count: subElement.doc_count
            });
        }
        acc.push({
            type: element.key,
            count: element.doc_count,
            sous_types: subAcc
        })
    }
    return acc;
}

exports.statsByMonth = (client, callback) => {
    client
        .search({
            index: 'incidents',
            body: {
                "aggs": {
                    "anomaliesParMois": {
                        "date_histogram": {
                            "field": "timestamp",
                            "calendar_interval": "month",
                            "format": "MM/yyyy",
                            "order": {
                                "_count": "desc"
                            }
                        }
                    }
                }
            }
        })
        .then(resp => callback(formatStatMonth(resp.body.aggregations.anomaliesParMois.buckets)), err => console.error(err.meta.body.error));
}

//mets en forme le résultat comme demandé dans l'exercice
function formatStatMonth(data) {
    let acc = [];
    for (let index = 0; index < data.length; index++) {
        const element = data[index];
        acc.push({
            month: element.key_as_string,
            count: element.doc_count
        })
    }
    return acc.slice(0, 10);
}

// BY MONTH
// PREMIERE METHODE AVEC DEUX AGGREGATIONS

/*exports.statsByMonth = (client, callback) => {
    client
        .search({
            index: 'incidents',
            body: {
                aggs: {
                    "annees": {
                        "terms": {
                            "field": "annee_declaration.keyword",
                            size: 500
                        },
                        aggs: {
                            "months": {
                                "terms": {
                                    "field": "mois_declaration.keyword",
                                    size: 10,
                                }
                            }
                        }
                    }
                }
            }
        })
        .then(resp => callback(formatStatMonth(resp.body.aggregations)
        ), err => console.error(err.meta.body.error));
}

function formatStatMonth(data) {
    let acc = [];
    for (let index = 0; index < data.annees.buckets.length; index++) {
        const current = data.annees.buckets[index];
        const annee = current.key;
        for (let indexMonth = 0; indexMonth < current.months.buckets.length; indexMonth++) {
            const currentMonth = current.months.buckets[indexMonth];
            const month = currentMonth.key
            acc.push({
                month: month + '/' + annee,
                count: currentMonth.doc_count
            })
        }
    }
    return acc.sort((a, b) => b.count - a.count).slice(0, 10);
}*/

exports.statsPropreteByArrondissement = (client, callback) => {
    client
        .search({
            index: 'incidents',
            body: {
                aggs: {
                    "arrondissements": {
                        "terms": {
                            "field": "arrondissement.keyword",
                            size:3
                        }
                    }
                },
                query: {
                    "bool": {
                        "must": [{
                            "match": {
                                "type": "Propreté"
                            }
                        }]
                    }
                }
            }
        })
        .then(resp => callback(formatStatPropreteArrondissement(resp.body.aggregations.arrondissements.buckets)
        ), err => console.error(err.meta.body.error));
}

//mets en forme le résultat comme demandé dans l'exercice
function formatStatPropreteArrondissement(data) {
    let acc = [];
    for (let index = 0; index < data.length; index++) {
        const element = data[index];
        acc.push({
            arrondissements: element.key,
            count: element.doc_count
        })
    }
    return acc;
}