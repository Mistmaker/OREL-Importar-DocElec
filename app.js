const path = require('path');
const fs = require('fs');
var he = require('he');

const sqlite3 = require('sqlite3').verbose();
var parser = require('fast-xml-parser');
const { ExisteProveedor, ExisteDocumento } = require('./utils');

const rutaArchivos = process.argv[2];
// const ordenNombre = process.argv[3];

let db = new sqlite3.Database('sriAlexis.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    // console.log('Connected to the sri database.');
});

const directoryPath = rutaArchivos;

let archivosEnviados = 0;
let archivosCargados = 0;
let archivosRepetidos = 0;
let archivosNoAdmitidos = 0;

// OPCIONES DE CONVERSION DE XML
var options = {
    attributeNamePrefix: "@_",
    attrNodeName: "attr", //default is 'false'
    textNodeName: "#text",
    ignoreAttributes: false,
    ignoreNameSpace: false,
    parseTrueNumberOnly: true,
};

async function LeerCarpeta(rutaCarpeta) {
    return new Promise((resolve, reject) => {
        fs.readdir(rutaCarpeta, async function (err, files) {
            if (err) {
                return console.log('No se pudo acceder al directorio/carpeta: ' + err);
            }

            for (const file of files) {
                const ruta = path.join(rutaCarpeta, file);
                if (fs.lstatSync(ruta).isDirectory()) { await LeerCarpeta(ruta); } else {
                    if (ruta.includes('.xml')) {
                        archivosEnviados += 1;
                        const filePath = path.join(rutaCarpeta, file);
                        const resp = await abrirArchivoXml(filePath);
                    }
                }
            }
            for (const file of files) {
                const ruta = path.join(rutaCarpeta, file);
                if (fs.lstatSync(ruta).isDirectory()) { await LeerCarpeta(ruta); } else {
                    if (ruta.includes('.xml')) {
                        const filePath = path.join(rutaCarpeta, file);
                        const resp = await abrirArchivoXml(filePath, 'p');
                        // // Renombrado de archivo
                        // fs.rename(filePath, rutaCarpeta + '/' + resp, function (err) {
                        //     if (err) console.log('ERROR: ' + err);
                        // });
                    }
                }
            }
            resolve(true);
        });

    });
}

async function abrirArchivoXml(filePath, action = 'd') {
    return new Promise((resolve, reject) => {
        fs.readFile(filePath, 'utf8', async (err, data) => {
            if (err) throw err;
            nuevoNombre = '';
            xmlData = data;
            try {
                // var documentoElectronico = parser.parse(xmlData, {ignoreAttributes: false, ignoreNameSpace: false , attributeNamePrefix : "@_",attrNodeName: "nombre",  parseTrueNumberOnly: true, attrValueProcessor: (val, attrName) => he.decode(val, {isAttributeValue: true}) , tagValueProcessor : (val, tagName) => he.decode(val)}, true);
                // var documento = parser.parse(documentoElectronico.autorizacion.comprobante, { parseTrueNumberOnly: true }, true);
                try {
                    xmlData = xmlData.replace(/&amp;/g, '').replace(/&quot;/g, '');
                    var temp = xmlData;
                    var count = (temp.match(/]]>/g) || []).length;
                    if (count > 1) xmlData = xmlData.replace(']]>', ']]]]><![CDATA[>');
                    var documentoElectronico = parser.parse(xmlData, options, true);
                    var documento = parser.parse(he.decode(documentoElectronico.autorizacion.comprobante), options, true);
                } catch (error) {
                    console.log('error convert xml=>', error)
                }

                if (documento.factura) {
                    documento.factura.numeroAutorizacion = documentoElectronico.autorizacion.numeroAutorizacion;
                    documento.factura.fechaAutorizacion = documentoElectronico.autorizacion.fechaAutorizacion;
                    if (action == 'd') await CargarFactura(documento.factura);
                    if (action !== 'd') await CargarProveedoorFactura(documento.factura);
                    // if (action !== 'd') nuevoNombre = GenerarNuevoNombreXml(documento.factura, documento.factura.infoFactura);
                } else if (documento.notaCredito) {
                    documento.notaCredito.numeroAutorizacion = documentoElectronico.autorizacion.numeroAutorizacion;
                    documento.notaCredito.fechaAutorizacion = documentoElectronico.autorizacion.fechaAutorizacion;
                    if (action == 'd') await CargarNotaCredito(documento.notaCredito);
                    if (action !== 'd') await CargarProveedorNotaCredito(documento.notaCredito);
                    // if (action !== 'd') nuevoNombre = GenerarNuevoNombreXml(documento.notaCredito, documento.notaCredito.infoNotaCredito);
                } else if (documento.comprobanteRetencion) {
                    documento.comprobanteRetencion.numeroAutorizacion = documentoElectronico.autorizacion.numeroAutorizacion;
                    documento.comprobanteRetencion.fechaAutorizacion = documentoElectronico.autorizacion.fechaAutorizacion;
                    if (action == 'd') await CargarRetencion(documento.comprobanteRetencion);
                    if (action !== 'd') await CargarProveedorRetencion(documento.comprobanteRetencion);
                    // if (action !== 'd') nuevoNombre = GenerarNuevoNombreXml(documento.comprobanteRetencion, documento.comprobanteRetencion.infoCompRetencion);
                } else if (documento.notaDebito) {
                    documento.notaDebito.numeroAutorizacion = documentoElectronico.autorizacion.numeroAutorizacion;
                    documento.notaDebito.fechaAutorizacion = documentoElectronico.autorizacion.fechaAutorizacion;
                    if (action == 'd') await CargarNotaDebito(documento.notaDebito);
                    if (action !== 'd') await CargarProveedorNotaDebito(documento.notaDebito);
                    // if (action !== 'd') nuevoNombre = GenerarNuevoNombreXml(documento.notaDebito, documento.notaDebito.infoNotaDebito);
                } else {
                    archivosNoAdmitidos += 1;
                    console.log('Documento no admitido');
                }

                // resolve(nuevoNombre);
                resolve(true);
            } catch (error) {
                return reject('error abrirArchivoXml =>', error.message)
            }
        });
    });
}

const IniciarCarga = async _ => {
    const resp = await LeerCarpeta(directoryPath);
    console.log(`Archivos cargados con exito: Enviados=${archivosEnviados} | Cargados=${archivosCargados} | Repetidos=${archivosRepetidos}`);
    // setTimeout(() => {
    db.close();
    // }, 2000);
}

async function CargarFactura(xml) {
    var existeFac = await ExisteDocumento(xml.numeroAutorizacion, xml.fechaAutorizacion, 'fac_cab')
    if (existeFac) { console.log('Factura ya registrada'); archivosRepetidos += 1; return; }

    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoFactura.identificacionComprador.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoFactura.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoFactura.identificacionComprador.toString()]);
                stmt.finalize();
            });
        }

        if (!xml.infoTributaria.nombreComercial) { xml.infoTributaria.nombreComercial = ''; }
        if (!xml.infoFactura.direccionComprador) { xml.infoFactura.direccionComprador = ''; }
        if (!xml.infoFactura.contribuyenteEspecial) { xml.infoFactura.contribuyenteEspecial = ''; }
        if (!xml.infoFactura.totalSubsidio) { xml.infoFactura.totalSubsidio = ''; }
        if (!xml.infoFactura.propina) { xml.infoFactura.propina = ''; }
        if (!xml.infoFactura.moneda) { xml.infoFactura.moneda = ''; }

        db.serialize(function () {
            var stmt = db.prepare(`INSERT INTO fac_cab (numeroAutorizacion,fechaAutorizacion,ambiente,estado,ambientec,tipoEmision,razonSocial,nombreComercial,ruc,claveAcceso,codDoc,estab,ptoEmi,secuencial,dirMatriz,fechaEmision,dirEstablecimiento,contribuyenteEspecial,obligadoContabilidad,tipoIdentificacionComprador,razonSocialComprador,identificacionComprador,direccionComprador,totalSinImpuestos,totalSubsidio,totalDescuento,propina,importeTotal,moneda,categoria) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
            stmt.run([xml.numeroAutorizacion, xml.fechaAutorizacion, 'PRODUCCION', 'AUTORIZADO', xml.infoTributaria.ambiente, xml.infoTributaria.tipoEmision, xml.infoTributaria.razonSocial.toString(), xml.infoTributaria.nombreComercial.toString(), xml.infoTributaria.ruc.toString(), xml.infoTributaria.claveAcceso, xml.infoTributaria.codDoc, xml.infoTributaria.estab, xml.infoTributaria.ptoEmi, xml.infoTributaria.secuencial, xml.infoTributaria.dirMatriz, xml.infoFactura.fechaEmision, xml.infoFactura.dirEstablecimiento, xml.infoFactura.contribuyenteEspecial, xml.infoFactura.obligadoContabilidad, xml.infoFactura.tipoIdentificacionComprador, xml.infoFactura.razonSocialComprador, xml.infoFactura.identificacionComprador.toString(), xml.infoFactura.direccionComprador, xml.infoFactura.totalSinImpuestos, xml.infoFactura.totalSubsidio, xml.infoFactura.totalDescuento, xml.infoFactura.propina, xml.infoFactura.importeTotal, xml.infoFactura.moneda, null]);
            stmt.finalize();
        });

        var sec = 1;
        for (const impuestos of Object.entries(xml.infoFactura.totalConImpuestos)) {
            if (impuestos[1].length) {
                for (const impu of Object.entries(impuestos[1])) {
                    var impuesto = impu[1];
                    var stmt = db.prepare(`INSERT INTO fac_cab_tci VALUES (?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoPorcentaje, impuesto.baseImponible, impuesto.valor]);
                    stmt.finalize();
                    sec += 1;
                }
            } else {
                var impuesto = impuestos[1];
                var stmt = db.prepare(`INSERT INTO fac_cab_tci VALUES (?,?,?,?,?,?,?)`);
                stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoPorcentaje, impuesto.baseImponible, impuesto.valor]);
                stmt.finalize();
                sec += 1;

            }
        }

        var sec = 1;
        for (const detalles of Object.entries(xml.detalles)) {
            // console.log(detalles[1]);
            if (detalles[1].length) {
                // console.log('multi', detalles[1]);
                for (const deta of Object.entries(detalles[1])) {
                    var detalle = deta[1];
                    if (!detalle.precioSinSubsidio) { detalle.precioSinSubsidio = 0; }
                    if (!detalle.codigoPrincipal || detalle.codigoPrincipal === null) { detalle.codigoPrincipal = ''; }
                    if (!detalle.codigoAuxiliar) { detalle.codigoAuxiliar = ''; }
                    // console.log(detalle);
                    var stmt = db.prepare(`INSERT INTO fac_det VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), detalle.descripcion, detalle.cantidad, detalle.precioUnitario, detalle.precioSinSubsidio, detalle.descuento, detalle.precioTotalSinImpuesto]);
                    stmt.finalize();
                    sec += 1;
                    // console.log(detalle);

                    if (detalle.impuestos) {
                        var secImp = 1;
                        for (const impuestos of Object.entries(detalle.impuestos)) {
                            if (impuestos[1].length) {
                                for (const impuesto of Object.entries(impuestos[1])) {
                                    var imp = impuesto[1];
                                    var stmt = db.prepare(`INSERT INTO fac_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                                    stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                                    stmt.finalize();
                                    secImp += 1;
                                }
                            } else {
                                var imp = impuestos[1];
                                var stmt = db.prepare(`INSERT INTO fac_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                                stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                                stmt.finalize();
                                secImp += 1;
                            }
                        }
                    }

                    if (detalle.detallesAdicionales) {
                        // console.log('multi', detalle.detallesAdicionales);
                        var secDetAdi = 1;
                        for (const detallesAdicionales of Object.entries(detalle.detallesAdicionales)) {
                            // console.log(detallesAdicionales[1]);
                            if (detallesAdicionales[1].length) {
                                for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                                    var det = detalleAdicional[1];
                                    // console.log(det);
                                    // console.log(det['attr']['@_nombre']);
                                    // console.log(det['attr']['@_valor']);
                                    //fac_det_detadi
                                    var stmt = db.prepare(`INSERT INTO fac_det_detadi VALUES (?,?,?,?,?,?,?)`);
                                    // console.log('det multi adi multi',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                    stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                    stmt.finalize();
                                    secDetAdi += 1;
                                }
                            } else {
                                var det = detallesAdicionales[1];
                                // console.log(det);
                                // console.log(det['attr']['@_nombre']);
                                // console.log(det['attr']['@_valor']);
                                //fac_det_detadi
                                var stmt = db.prepare(`INSERT INTO fac_det_detadi VALUES (?,?,?,?,?,?,?)`);
                                // console.log('det multi adi one',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);

                                stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                stmt.finalize();
                                secDetAdi += 1;
                            }
                        }
                    }
                    // console.log('======');
                }
            } else {
                // console.log('one', detalles[1]);
                var detalle = detalles[1];
                if (!detalle.precioSinSubsidio) { detalle.precioSinSubsidio = 0; }
                if (!detalle.codigoAuxiliar) { detalle.codigoAuxiliar = ''; }
                if (!detalle.codigoPrincipal) { detalle.codigoPrincipal = ''; }
                var stmt = db.prepare(`INSERT INTO fac_det VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
                stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), detalle.descripcion, detalle.cantidad, detalle.precioUnitario, detalle.precioSinSubsidio, detalle.descuento, detalle.precioTotalSinImpuesto]);
                stmt.finalize();
                sec += 1;
                // console.log(detalle);

                if (detalle.impuestos) {
                    var secImp = 1;
                    for (const impuestos of Object.entries(detalle.impuestos)) {
                        if (impuestos[1].length) {
                            for (const impuesto of Object.entries(impuestos[1])) {
                                var imp = impuesto[1];
                                var stmt = db.prepare(`INSERT INTO fac_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                                stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                                stmt.finalize();
                                secImp += 1;
                            }
                        } else {
                            var imp = impuestos[1];
                            var stmt = db.prepare(`INSERT INTO fac_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                            stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                            stmt.finalize();
                            secImp += 1;
                        }
                    }
                }

                if (detalle.detallesAdicionales) {
                    // console.log('multi', detalle.detallesAdicionales);
                    var secDetAdi = 1;
                    for (const detallesAdicionales of Object.entries(detalle.detallesAdicionales)) {
                        // console.log(detallesAdicionales[1]);
                        if (detallesAdicionales[1].length) {
                            for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                                var det = detalleAdicional[1];
                                // console.log(det);
                                // console.log(det['attr']['@_nombre']);
                                // console.log(det['attr']['@_valor']);
                                //fac_det_detadi
                                var stmt = db.prepare(`INSERT INTO fac_det_detadi VALUES (?,?,?,?,?,?,?)`);
                                // console.log('det one adi multi',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                stmt.finalize();
                                secDetAdi += 1;
                            }
                        } else {
                            var det = detallesAdicionales[1];
                            // console.log(det);
                            // console.log(det['attr']['@_nombre']);
                            // console.log(det['attr']['@_valor']);
                            //fac_det_detadi
                            var stmt = db.prepare(`INSERT INTO fac_det_detadi VALUES (?,?,?,?,?,?,?)`);
                            // console.log('det one adi one',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                            stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                            stmt.finalize();
                            secDetAdi += 1;
                        }
                    }
                }
            }
        }

        if (xml.infoAdicional) {
            var secEncDetAdi = 1;
            for (const detallesAdicionales of Object.entries(xml.infoAdicional)) {
                // console.log(detallesAdicionales[1]);
                if (detallesAdicionales[1].length) {
                    for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                        var det = detalleAdicional[1];
                        var stmt = db.prepare(`INSERT INTO fac_cab_infadi VALUES (?,?,?,?,?)`);
                        stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                        stmt.finalize();
                        secEncDetAdi += 1;
                    }
                } else {
                    var det = detallesAdicionales[1];
                    var stmt = db.prepare(`INSERT INTO fac_cab_infadi VALUES (?,?,?,?,?)`);
                    stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                    stmt.finalize();
                    secEncDetAdi += 1;
                }
            }
        }

        archivosCargados += 1;

    } catch (error) {
        console.log('err CargarFactura =>', error)
    }

}

async function CargarNotaCredito(xml) {
    var existeNC = await ExisteDocumento(xml.numeroAutorizacion, xml.fechaAutorizacion, 'ncr_cab')
    if (existeNC) { console.log('Nota de crédito ya registrada'); archivosRepetidos += 1; return; }

    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoNotaCredito.identificacionComprador.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoNotaCredito.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoNotaCredito.identificacionComprador.toString()]);
                stmt.finalize();
            });
        }

        if (!xml.infoTributaria.nombreComercial) { xml.infoTributaria.nombreComercial = ''; }
        if (!xml.infoNotaCredito.direccionComprador) { xml.infoNotaCredito.direccionComprador = ''; }
        if (!xml.infoNotaCredito.contribuyenteEspecial) { xml.infoNotaCredito.contribuyenteEspecial = ''; }
        if (!xml.infoNotaCredito.moneda) { xml.infoNotaCredito.moneda = ''; }
        if (!xml.infoNotaCredito.motivo) { xml.infoNotaCredito.motivo = ''; }

        db.serialize(function () {
            var stmt = db.prepare(`INSERT INTO ncr_cab (numeroAutorizacion,fechaAutorizacion,ambiente,estado,ambientec,tipoEmision,razonSocial,nombreComercial,ruc,claveAcceso,codDoc,estab,ptoEmi,secuencial,dirMatriz,fechaEmision,dirEstablecimiento,tipoIdentificacionComprador,razonSocialComprador,identificacionComprador,contribuyenteEspecial,obligadoContabilidad,codDocModificado,numDocModificado,fechaEmisionDocSustento,totalSinImpuestos,valorModificacion,moneda,motivo,categoria) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
            stmt.run([xml.numeroAutorizacion, xml.fechaAutorizacion, 'PRODUCCION', 'AUTORIZADO', xml.infoTributaria.ambiente, xml.infoTributaria.tipoEmision, xml.infoTributaria.razonSocial.toString(), xml.infoTributaria.nombreComercial.toString(), xml.infoTributaria.ruc.toString(), xml.infoTributaria.claveAcceso, xml.infoTributaria.codDoc, xml.infoTributaria.estab, xml.infoTributaria.ptoEmi, xml.infoTributaria.secuencial, xml.infoTributaria.dirMatriz, xml.infoNotaCredito.fechaEmision, xml.infoNotaCredito.dirEstablecimiento, xml.infoNotaCredito.tipoIdentificacionComprador, xml.infoNotaCredito.razonSocialComprador, xml.infoNotaCredito.identificacionComprador.toString(), xml.infoNotaCredito.contribuyenteEspecial, xml.infoNotaCredito.obligadoContabilidad, xml.infoNotaCredito.codDocModificado, xml.infoNotaCredito.numDocModificado, xml.infoNotaCredito.fechaEmisionDocSustento, xml.infoNotaCredito.totalSinImpuestos, xml.infoNotaCredito.valorModificacion, xml.infoNotaCredito.moneda, xml.infoNotaCredito.motivo, null]);
            stmt.finalize();
        });

        var sec = 1;
        for (const impuestos of Object.entries(xml.infoNotaCredito.totalConImpuestos)) {
            if (impuestos[1].length) {
                for (const impu of Object.entries(impuestos[1])) {
                    var impuesto = impu[1];
                    var stmt = db.prepare(`INSERT INTO ncr_cab_tci VALUES (?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoPorcentaje, impuesto.baseImponible, impuesto.valor]);
                    stmt.finalize();
                    sec += 1;
                }
            } else {
                var impuesto = impuestos[1];
                var stmt = db.prepare(`INSERT INTO ncr_cab_tci VALUES (?,?,?,?,?,?,?)`);
                stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoPorcentaje, impuesto.baseImponible, impuesto.valor]);
                stmt.finalize();
                sec += 1;
            }
        }

        var sec = 1;
        for (const detalles of Object.entries(xml.detalles)) {
            // console.log(detalles[1]);
            if (detalles[1].length) {
                // console.log('multi', detalles[1]);
                for (const deta of Object.entries(detalles[1])) {
                    var detalle = deta[1];
                    if (!detalle.codigoInterno || detalle.codigoInterno === null) { detalle.codigoInterno = ''; }
                    if (!detalle.codigoAdicional) { detalle.codigoAdicional = ''; }
                    // console.log(detalle);
                    var stmt = db.prepare(`INSERT INTO ncr_det VALUES (?,?,?,?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), detalle.descripcion, detalle.cantidad, detalle.precioUnitario, detalle.descuento, detalle.precioTotalSinImpuesto]);
                    stmt.finalize();
                    sec += 1;
                    // console.log(detalle);

                    if (detalle.impuestos) {
                        var secImp = 1;
                        for (const impuestos of Object.entries(detalle.impuestos)) {
                            if (impuestos[1].length) {
                                for (const impuesto of Object.entries(impuestos[1])) {
                                    var imp = impuesto[1];
                                    var stmt = db.prepare(`INSERT INTO ncr_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                                    stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                                    stmt.finalize();
                                    secImp += 1;
                                }
                            } else {
                                var imp = impuestos[1];
                                var stmt = db.prepare(`INSERT INTO ncr_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                                stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                                stmt.finalize();
                                secImp += 1;
                            }
                        }
                    }

                    if (detalle.detallesAdicionales) {
                        // console.log('multi', detalle.detallesAdicionales);
                        var secDetAdi = 1;
                        for (const detallesAdicionales of Object.entries(detalle.detallesAdicionales)) {
                            // console.log(detallesAdicionales[1]);
                            if (detallesAdicionales[1].length) {
                                for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                                    var det = detalleAdicional[1];
                                    // console.log(det);
                                    // console.log(det['attr']['@_nombre']);
                                    // console.log(det['attr']['@_valor']);
                                    //ncr_det_detadi
                                    var stmt = db.prepare(`INSERT INTO ncr_det_detadi VALUES (?,?,?,?,?,?,?)`);
                                    // console.log('det multi adi multi',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                    stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                    stmt.finalize();
                                    secDetAdi += 1;
                                }
                            } else {
                                var det = detallesAdicionales[1];
                                // console.log(det);
                                // console.log(det['attr']['@_nombre']);
                                // console.log(det['attr']['@_valor']);
                                //ncr_det_detadi
                                var stmt = db.prepare(`INSERT INTO ncr_det_detadi VALUES (?,?,?,?,?,?,?)`);
                                // console.log('det multi adi one',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);

                                stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                stmt.finalize();
                                secDetAdi += 1;
                            }
                        }
                    }
                    // console.log('======');
                }
            } else {
                // console.log('one', detalles[1]);
                var detalle = detalles[1];
                if (!detalle.codigoInterno || detalle.codigoInterno === null) { detalle.codigoInterno = ''; }
                if (!detalle.codigoAdicional) { detalle.codigoAdicional = ''; }
                var stmt = db.prepare(`INSERT INTO ncr_det VALUES (?,?,?,?,?,?,?,?,?,?)`);
                stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), detalle.descripcion, detalle.cantidad, detalle.precioUnitario, detalle.descuento, detalle.precioTotalSinImpuesto]);
                stmt.finalize();
                sec += 1;
                // console.log(detalle);

                if (detalle.impuestos) {
                    var secImp = 1;
                    for (const impuestos of Object.entries(detalle.impuestos)) {
                        if (impuestos[1].length) {
                            for (const impuesto of Object.entries(impuestos[1])) {
                                var imp = impuesto[1];
                                var stmt = db.prepare(`INSERT INTO ncr_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                                stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                                stmt.finalize();
                                secImp += 1;
                            }
                        } else {
                            var imp = impuestos[1];
                            var stmt = db.prepare(`INSERT INTO ncr_det_imp VALUES (?,?,?,?,?,?,?,?,?,?)`);
                            stmt.run([secImp, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), imp.codigo, imp.codigoPorcentaje, imp.tarifa, imp.baseImponible, imp.valor]);
                            stmt.finalize();
                            secImp += 1;
                        }
                    }
                }

                if (detalle.detallesAdicionales) {
                    // console.log('multi', detalle.detallesAdicionales);
                    var secDetAdi = 1;
                    for (const detallesAdicionales of Object.entries(detalle.detallesAdicionales)) {
                        // console.log(detallesAdicionales[1]);
                        if (detallesAdicionales[1].length) {
                            for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                                var det = detalleAdicional[1];
                                // console.log(det);
                                // console.log(det['attr']['@_nombre']);
                                // console.log(det['attr']['@_valor']);
                                //fac_det_detadi
                                var stmt = db.prepare(`INSERT INTO ncr_det_detadi VALUES (?,?,?,?,?,?,?)`);
                                // console.log('det one adi multi',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                                stmt.finalize();
                                secDetAdi += 1;
                            }
                        } else {
                            var det = detallesAdicionales[1];
                            // console.log(det);
                            // console.log(det['attr']['@_nombre']);
                            // console.log(det['attr']['@_valor']);
                            //fac_det_detadi
                            var stmt = db.prepare(`INSERT INTO ncr_det_detadi VALUES (?,?,?,?,?,?,?)`);
                            // console.log('det one adi one',[secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoPrincipal.toString(), detalle.codigoAuxiliar.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                            stmt.run([secDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, detalle.codigoInterno.toString(), detalle.codigoAdicional.toString(), det['attr']['@_nombre'], det['attr']['@_valor']]);
                            stmt.finalize();
                            secDetAdi += 1;
                        }
                    }
                }
            }
        }

        if (xml.infoAdicional) {
            var secEncDetAdi = 1;
            for (const detallesAdicionales of Object.entries(xml.infoAdicional)) {
                // console.log(detallesAdicionales[1]);
                if (detallesAdicionales[1].length) {
                    for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                        var det = detalleAdicional[1];
                        var stmt = db.prepare(`INSERT INTO ncr_cab_infadi VALUES (?,?,?,?,?)`);
                        stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                        stmt.finalize();
                        secEncDetAdi += 1;
                    }
                } else {
                    var det = detallesAdicionales[1];
                    var stmt = db.prepare(`INSERT INTO ncr_cab_infadi VALUES (?,?,?,?,?)`);
                    stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                    stmt.finalize();
                    secEncDetAdi += 1;
                }
            }
        }

        archivosCargados += 1;

    } catch (error) {
        console.log('err CargarNotaCredito=>', error)
    }
}

async function CargarRetencion(xml) {
    var existeNC = await ExisteDocumento(xml.numeroAutorizacion, xml.fechaAutorizacion, 'ret_cab')
    console.log(xml.numeroAutorizacion, xml.fechaAutorizacion,existeNC)
    if (existeNC) { console.log('Retención ya registrada'); archivosRepetidos += 1; return; }

    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoCompRetencion.identificacionSujetoRetenido.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoCompRetencion.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoCompRetencion.identificacionSujetoRetenido.toString()]);
                stmt.finalize();
            });
        }

        if (!xml.infoTributaria.nombreComercial) { xml.infoTributaria.nombreComercial = ''; }
        if (!xml.infoCompRetencion.contribuyenteEspecial) { xml.infoCompRetencion.contribuyenteEspecial = ''; }

        db.serialize(function () {
            var stmt = db.prepare(`INSERT INTO ret_cab (numeroAutorizacion,fechaAutorizacion,ambiente,estado,ambientec,tipoEmision,razonSocial,nombreComercial,ruc,claveAcceso,codDoc,estab,ptoEmi,secuencial,dirMatriz,fechaEmision,dirEstablecimiento,contribuyenteEspecial,obligadoContabilidad,tipoIdentificacionSujetoRetenido,razonSocialSujetoRetenido,identificacionSujetoRetenido,periodoFiscal) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
            stmt.run([xml.numeroAutorizacion, xml.fechaAutorizacion, 'PRODUCCION', 'AUTORIZADO', xml.infoTributaria.ambiente, xml.infoTributaria.tipoEmision, xml.infoTributaria.razonSocial.toString(), xml.infoTributaria.nombreComercial.toString(), xml.infoTributaria.ruc.toString(), xml.infoTributaria.claveAcceso, xml.infoTributaria.codDoc, xml.infoTributaria.estab, xml.infoTributaria.ptoEmi, xml.infoTributaria.secuencial, xml.infoTributaria.dirMatriz, xml.infoCompRetencion.fechaEmision, xml.infoCompRetencion.dirEstablecimiento, xml.infoCompRetencion.contribuyenteEspecial, xml.infoCompRetencion.obligadoContabilidad, xml.infoCompRetencion.tipoIdentificacionSujetoRetenido, xml.infoCompRetencion.razonSocialSujetoRetenido, xml.infoCompRetencion.identificacionSujetoRetenido.toString(), xml.infoCompRetencion.periodoFiscal]);
            stmt.finalize();
        });

        // Inicio Impuestos
        var sec = 1;
        if (xml.impuestos) {
            for (const impuestos of Object.entries(xml.impuestos)) {
                if (impuestos[1].length) {
                    for (const impu of Object.entries(impuestos[1])) {
                        var impuesto = impu[1];
                        var stmt = db.prepare(`INSERT INTO ret_det_imp VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
                        stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoRetencion, impuesto.baseImponible, impuesto.porcentajeRetener, impuesto.valorRetenido, impuesto.codDocSustento, impuesto.numDocSustento, impuesto.fechaEmisionDocSustento]);
                        stmt.finalize();
                        sec += 1;
                    }
                } else {
                    var impuesto = impuestos[1];
                    var stmt = db.prepare(`INSERT INTO ret_det_imp VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoRetencion, impuesto.baseImponible, impuesto.porcentajeRetener, impuesto.valorRetenido, impuesto.codDocSustento, impuesto.numDocSustento, impuesto.fechaEmisionDocSustento]);
                    stmt.finalize();
                    sec += 1;
                }
            }
        }

        var sec = 1;
        if (xml.docsSustento) {
            const codDocSustento = xml.docsSustento.docSustento.codDocSustento;
            const numDocSustento = xml.docsSustento.docSustento.numDocSustento;
            const fechaEmisionDocSustento = xml.docsSustento.docSustento.fechaEmisionDocSustento;
            // console.log({ codDocSustento, numDocSustento, fechaEmisionDocSustento });
            for (const impuestos of Object.entries(xml.docsSustento.docSustento.retenciones)) {
                if (impuestos[1].length) {
                    for (const impu of Object.entries(impuestos[1])) {
                        var impuesto = impu[1];
                        var stmt = db.prepare(`INSERT INTO ret_det_imp VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
                        stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoRetencion, impuesto.baseImponible, impuesto.porcentajeRetener, impuesto.valorRetenido, codDocSustento, numDocSustento, fechaEmisionDocSustento]);
                        stmt.finalize();
                        sec += 1;
                    }
                } else {
                    var impuesto = impuestos[1];
                    var stmt = db.prepare(`INSERT INTO ret_det_imp VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoRetencion, impuesto.baseImponible, impuesto.porcentajeRetener, impuesto.valorRetenido, codDocSustento, numDocSustento, fechaEmisionDocSustento]);
                    stmt.finalize();
                    sec += 1;
                }
            }
        }

        // Fin Impuestos

        if (xml.infoAdicional) {
            var secEncDetAdi = 1;
            for (const detallesAdicionales of Object.entries(xml.infoAdicional)) {
                // console.log(detallesAdicionales[1]);
                if (detallesAdicionales[1].length) {
                    for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                        var det = detalleAdicional[1];
                        var stmt = db.prepare(`INSERT INTO ret_cab_infadi VALUES (?,?,?,?,?)`);
                        stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                        stmt.finalize();
                        secEncDetAdi += 1;
                    }
                } else {
                    var det = detallesAdicionales[1];
                    var stmt = db.prepare(`INSERT INTO ret_cab_infadi VALUES (?,?,?,?,?)`);
                    stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                    stmt.finalize();
                    secEncDetAdi += 1;
                }
            }
        }

        archivosCargados += 1;

    } catch (error) {
        console.log('err CargarRetencion=>', error)
    }
}

async function CargarNotaDebito(xml) {
    var existeNC = await ExisteDocumento(xml.numeroAutorizacion, xml.fechaAutorizacion, 'ndb_cab')
    if (existeNC) { console.log('Nota de Debito ya registrada'); archivosRepetidos += 1; return; }

    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoNotaDebito.identificacionComprador.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoNotaDebito.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoNotaDebito.identificacionComprador.toString()]);
                stmt.finalize();
            });
        }

        if (!xml.infoTributaria.nombreComercial) { xml.infoTributaria.nombreComercial = ''; }
        if (!xml.infoNotaDebito.direccionComprador) { xml.infoNotaDebito.direccionComprador = ''; }
        if (!xml.infoNotaDebito.contribuyenteEspecial) { xml.infoNotaDebito.contribuyenteEspecial = ''; }
        if (!xml.infoNotaDebito.moneda) { xml.infoNotaDebito.moneda = ''; }
        if (!xml.infoNotaDebito.motivo) { xml.infoNotaDebito.motivo = ''; }

        db.serialize(function () {
            var stmt = db.prepare(`INSERT INTO ndb_cab (numeroAutorizacion,fechaAutorizacion,ambiente,estado,ambientec,tipoEmision,razonSocial,nombreComercial,ruc,claveAcceso,codDoc,estab,ptoEmi,secuencial,dirMatriz,fechaEmision,dirEstablecimiento,tipoIdentificacionComprador,razonSocialComprador,identificacionComprador,contribuyenteEspecial,obligadoContabilidad,codDocModificado,numDocModificado,fechaEmisionDocSustento,totalSinImpuestos,valorModificacion,moneda,motivo,categoria) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
            stmt.run([xml.numeroAutorizacion, xml.fechaAutorizacion, 'PRODUCCION', 'AUTORIZADO', xml.infoTributaria.ambiente, xml.infoTributaria.tipoEmision, xml.infoTributaria.razonSocial.toString(), xml.infoTributaria.nombreComercial.toString(), xml.infoTributaria.ruc.toString(), xml.infoTributaria.claveAcceso, xml.infoTributaria.codDoc, xml.infoTributaria.estab, xml.infoTributaria.ptoEmi, xml.infoTributaria.secuencial, xml.infoTributaria.dirMatriz, xml.infoNotaDebito.fechaEmision, xml.infoNotaDebito.dirEstablecimiento, xml.infoNotaDebito.tipoIdentificacionComprador, xml.infoNotaDebito.razonSocialComprador, xml.infoNotaDebito.identificacionComprador.toString(), xml.infoNotaDebito.contribuyenteEspecial, xml.infoNotaDebito.obligadoContabilidad, xml.infoNotaDebito.codDocModificado, xml.infoNotaDebito.numDocModificado, xml.infoNotaDebito.fechaEmisionDocSustento, xml.infoNotaDebito.totalSinImpuestos, xml.infoNotaDebito.valorModificacion, xml.infoNotaDebito.moneda, xml.infoNotaDebito.motivo, null]);
            stmt.finalize();
        });

        var sec = 1;
        for (const impuestos of Object.entries(xml.infoNotaDebito.impuestos)) {
            if (impuestos[1].length) {
                for (const impu of Object.entries(impuestos[1])) {
                    var impuesto = impu[1];
                    var stmt = db.prepare(`INSERT INTO ndb_det_imp VALUES (?,?,?,?,?,?,?,?)`);
                    stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoPorcentaje, impuesto.tarifa, impuesto.baseImponible, impuesto.valor]);
                    stmt.finalize();
                    sec += 1;
                }
            } else {
                var impuesto = impuestos[1];
                var stmt = db.prepare(`INSERT INTO ndb_det_imp VALUES (?,?,?,?,?,?,?,?)`);
                stmt.run([sec, xml.numeroAutorizacion, xml.fechaAutorizacion, impuesto.codigo, impuesto.codigoPorcentaje, impuesto.tarifa, impuesto.baseImponible, impuesto.valor]);
                stmt.finalize();
                sec += 1;
            }
        }

        var secMotivos = 1;
        for (const motivos of Object.entries(xml.motivos)) {
            if (motivos[1].length) {
                for (const moti of Object.entries(motivos[1])) {
                    var motivo = moti[1];
                    var stmt = db.prepare(`INSERT INTO ndb_det VALUES (?,?,?,?,?)`);
                    stmt.run([secMotivos, xml.numeroAutorizacion, xml.fechaAutorizacion, motivo.razon, motivo.valor]);
                    stmt.finalize();
                    secMotivos += 1;
                }
            } else {
                var motivo = motivos[1];
                var stmt = db.prepare(`INSERT INTO ndb_det VALUES (?,?,?,?,?)`);
                stmt.run([secMotivos, xml.numeroAutorizacion, xml.fechaAutorizacion, motivo.razon, motivo.valor]);
                stmt.finalize();
                secMotivos += 1;
            }
        }

        if (xml.infoAdicional) {
            var secEncDetAdi = 1;
            for (const detallesAdicionales of Object.entries(xml.infoAdicional)) {
                // console.log(detallesAdicionales[1]);
                if (detallesAdicionales[1].length) {
                    for (const detalleAdicional of Object.entries(detallesAdicionales[1])) {
                        var det = detalleAdicional[1];
                        var stmt = db.prepare(`INSERT INTO ndb_cab_infadi VALUES (?,?,?,?,?)`);
                        stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                        stmt.finalize();
                        secEncDetAdi += 1;
                    }
                } else {
                    var det = detallesAdicionales[1];
                    var stmt = db.prepare(`INSERT INTO ndb_cab_infadi VALUES (?,?,?,?,?)`);
                    stmt.run([secEncDetAdi, xml.numeroAutorizacion, xml.fechaAutorizacion, det['attr']['@_nombre'], det["#text"]]);
                    stmt.finalize();
                    secEncDetAdi += 1;
                }
            }
        }

        archivosCargados += 1;

    } catch (error) {
        console.log('err CargarNotaDebito=>', error)
    }
}

async function CargarProveedoorFactura(xml) {
    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoFactura.identificacionComprador.toString());
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoFactura.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoFactura.identificacionComprador.toString()]);
                stmt.finalize();
            });
        }

    } catch (error) {
        console.log('err CargarProveedoorFactura=>', error)
    }

}

async function CargarProveedorNotaCredito(xml) {
    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoNotaCredito.identificacionComprador.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoNotaCredito.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoNotaCredito.identificacionComprador.toString()]);
                stmt.finalize();
            });
        }

    } catch (error) {
        console.log('err CargarProveedorNotaCredito=>', error)
    }
}

async function CargarProveedorRetencion(xml) {
    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoCompRetencion.identificacionSujetoRetenido.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoCompRetencion.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoCompRetencion.identificacionSujetoRetenido.toString()]);
                stmt.finalize();
            });
        }

    } catch (error) {
        console.log('err CargarProveedorRetencion=>', error)
    }
}

async function CargarProveedorNotaDebito(xml) {
    try {
        var existeProveedor = await ExisteProveedor(xml.infoTributaria.ruc.toString(), xml.infoNotaDebito.identificacionComprador.toString());
        existeProveedor = true;
        if (!existeProveedor) {
            //Crear Proveedor
            var tipoProveedor = '0';
            if (xml.infoTributaria.ruc.toString().length == 13) {
                tipoProveedor = "01";
            } else if (xml.infoTributaria.ruc.toString().length == 10) {
                tipoProveedor = "02";
            } else {
                tipoProveedor = "03";
            }
            db.serialize(function () {
                var stmt = db.prepare(`INSERT INTO com_proveedores (id_proveedor, tipo_proveedor, razonsocial_proveedor, actividad_proveedor,obligado_proveedor, nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?) `);
                stmt.run([xml.infoTributaria.ruc.toString(), tipoProveedor, xml.infoTributaria.razonSocial, '', xml.infoNotaDebito.obligadoContabilidad, xml.infoTributaria.nombreComercial, xml.infoNotaDebito.identificacionComprador.toString()]);
                stmt.finalize();
            });
        }

    } catch (error) {
        console.log('err CargarProveedorNotaDebito=>', error)
    }
}

// function GenerarNuevoNombreXml(xml, infoXml) {
//     ruc = xml.infoTributaria.ruc;
//     codDoc = xml.infoTributaria.codDoc;
//     estab = xml.infoTributaria.estab;
//     ptoEmi = xml.infoTributaria.ptoEmi;
//     secuencial = xml.infoTributaria.secuencial;
//     fechaEmision = infoXml.fechaEmision.replace(/\//g, '');
//     razonSocial = xml.infoTributaria.razonSocial;

//     console.log('Orden =>',ordenNombre);
//     // let orden = [2, 1, 4, 3, 5, 6, 7];
//     let orden = ordenNombre.split(',');
//     let datos = [];
//     datos[1] = xml.infoTributaria.ruc;
//     datos[2] = xml.infoTributaria.codDoc;
//     datos[3] = xml.infoTributaria.estab;
//     datos[4] = xml.infoTributaria.ptoEmi;
//     datos[5] = xml.infoTributaria.secuencial;
//     datos[6] = infoXml.fechaEmision.replace(/\//g, '');
//     datos[7] = xml.infoTributaria.razonSocial;

//     console.log(`${datos[orden[0]]}-${datos[orden[1]]}-${datos[orden[2]]}-${datos[orden[3]]}-${datos[orden[4]]}-${datos[orden[5]]}-${datos[orden[6]]}`);

//     // razonSocial = xml.infoTributaria.razonSocial.replace(/\s/g, '-');
//     // return `${ruc}-${codDoc}-${estab}-${ptoEmi}-${secuencial}-${fechaEmision}-${razonSocial}.xml`;
//     return `${datos[orden[0]]}-${datos[orden[1]]}-${datos[orden[2]]}-${datos[orden[3]]}-${datos[orden[4]]}-${datos[orden[5]]}-${datos[orden[6]]}.xml`;
// }

IniciarCarga();