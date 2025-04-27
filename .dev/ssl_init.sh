#!/bin/bash

export keystore_password="password";
export truststore_password="password";

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ssl_dir=${script_dir}/kafka/ssl;

mkcert_ca_root_key=$(mkcert -CAROOT)/rootCA-key.pem;
mkcert_ca_root=$(mkcert -CAROOT)/rootCA.pem;
mkdir -p ${ssl_dir};

rm -rf ${ssl_dir}/*;

# cat <<EOF > ${ssl_dir}/openssl.cnf
# [req]
# distinguished_name = req_distinguished_name
# req_extensions = v3_req
# prompt = no
#
# [req_distinguished_name]
# C = US
# ST = CA
# L = SF
# O = amountainram
# OU = rdkafka2
# CN = .dev
#
# [v3_req]
# subjectAltName = @alt_names
#
# [alt_names]
# DNS.1 = kafka
# DNS.2 = localhost
# DNS.3 = kafka-scram
# DNS.4 = kafka-oauthbearer
# IP.1 = 127.0.0.1
# IP.2 = ::1
# EOF

echo "Creating key + cert request for Kafka"
# openssl req \
# 	-new -newkey rsa:2048 \
# 	-keyout ${ssl_dir}/kafka-key.pem \
# 	-out ${ssl_dir}/kafka.csr \
# 	-config ${ssl_dir}/openssl.cnf \
# 	-passout pass: ;
# echo "Signing cert request for Kafka"
# openssl x509 \
# 	-req -in ${ssl_dir}/kafka.csr \
# 	-CA ${mkcert_ca_root} \
# 	-CAkey ${mkcert_ca_root_key} \
# 	-CAcreateserial \
# 	-out ${ssl_dir}/kafka-cert.pem \
# 	-days 365 \
# 	-extensions v3_req \
# 	-extfile ${ssl_dir}/openssl.cnf;
# echo "Remove csr"
mkcert -cert-file ${ssl_dir}/kafka-cert.pem -key-file ${ssl_dir}/kafka-key.pem \
	kafka localhost kafka-scram kafka-oauthbearer 127.0.0.1 ::1;
# rm ${ssl_dir}/kafka.csr ${ssl_dir}/openssl.cnf || true;
echo "Creating PKCS12 keystore for Kafka"
openssl pkcs12 \
	-export -in ${ssl_dir}/kafka-cert.pem \
	-inkey ${ssl_dir}/kafka-key.pem \
	-passin pass: \
	-out ${ssl_dir}/kafka.p12 \
	-passout env:keystore_password \
	-name kafka \
	-CAfile ${mkcert_ca_root} \
	-caname CARoot;
echo "Importing PKCS12 keystore into JKS keystore for Kafka"
keytool -importkeystore \
	-deststorepass "$keystore_password" \
	-destkeystore ${ssl_dir}/kafka.keystore.jks \
	-srckeystore ${ssl_dir}/kafka.p12 \
	-srcstoretype PKCS12 \
	-srcstorepass "$keystore_password" \
	-alias kafka;
echo "Remove keys"
rm ${ssl_dir}/kafka-key.pem ${ssl_dir}/kafka.p12 || true;
echo "Importing CA into truststore for Kafka"
keytool -keystore \
	${ssl_dir}/kafka.truststore.jks \
	-alias CARoot \
	-import -file ${mkcert_ca_root} \
	-storepass ${truststore_password} \
	-storetype JKS \
	-noprompt;

