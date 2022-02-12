const sqlite3 = require('sqlite3').verbose();

let db = new sqlite3.Database('sriAlexis.db', (err) => {
    if (err) { console.error(err.message); }
    // console.log('Connected to the sri database.');
});

const ExisteProveedor = (ruc, idComprador) => {
    return new Promise((resolve, reject) => {
        db.get(`SELECT * FROM com_proveedores where id_proveedor = '${ruc}' and informante = '${idComprador}'`, function (err, data) {
            if (data) {
                resolve(true);
            } else {
                resolve(false)
            }
        });
    });
}

const ExisteDocumento = (numAutorizacion, fecAutorizacion, tabla) => {
    return new Promise((resolve, reject) => {
        db.get(`SELECT * FROM ${tabla} where numeroAutorizacion = '${numAutorizacion}' and fechaAutorizacion = '${fecAutorizacion}'`, function (err, data) {
            if (data) {
                resolve(true);
            } else {
                resolve(false)
            }
        });
    });
}

module.exports = {
    ExisteProveedor,
    ExisteDocumento
};