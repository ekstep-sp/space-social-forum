/*               "Copyright 2020 Infosys Ltd.
               Use of this source code is governed by GPL v3 license that can be found in the LICENSE file or at https://opensource.org/licenses/GPL-3.0
               This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License version 3" */
const neo4j = require('neo4j-driver').v1;
const PropertiesReader = require('properties-reader')
const properties = PropertiesReader('./app.properties')

const neoUrl = process.env.neoUrl || properties.get('neoUrl')
const neoUsername = process.env.neoUsername || properties.get('neoUsername')
const neoPassword = process.env.neoPassword || properties.get('neoPassword')
let driver = neo4j.driver(neoUrl,neo4j.auth.basic(neoUsername, neoPassword));


function runQuery(query,params) {
    let neoSession = driver.session();
    return new Promise((resolve, reject) => {
        neoSession.run(query,params)
        .then((result) => {
            //log.info('Results: ' + result);
            resolve(result)
        }).catch((err) => {
            //log.error('error: ' + err);
            console.error('error: ' + err);
            reject(err)
        }).finally(() => {
            neoSession.close();
        })
    })
}

// on application exit:
driver.close();

module.exports = {
    runQuery
}
