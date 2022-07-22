# sd2021-tp2

Comandos importantes:

(Colocar a imagem docker a correr)<br/>
docker run -it --network sdnet sd2021-tp2-55247-yyyyy /bin/bash<br/>

(Colocar o servidor de utilizadores a correr)<br/>
java -cp sd2021.jar -Djavax.net.ssl.keyStore=userserver.ks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=truststore.ks -Djavax.net.ssl.trustStorePassword=changeit tp1.server.rest.UsersServer

(Colocar o servidor de spreadsheets com replicação a correr)<br/>
java -cp sd2021.jar -Djavax.net.ssl.keyStore=spreadsheetserver.ks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=truststore.ks -Djavax.net.ssl.trustStorePassword=changeit tp1.server.rest.replication.ReplicationSpreadsheetsServer

(Colocar o servidor de spreadsheets sem replicação a correr)<br/>
java -cp sd2021.jar -Djavax.net.ssl.keyStore=spreadsheetserver.ks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=truststore.ks -Djavax.net.ssl.trustStorePassword=changeit tp1.server.rest.SpreadsheetsServer

(Colocar uma classe a correr que serve de Cliente)<br/>
java -cp sd2021.jar -Djavax.net.ssl.trustStore=truststore.ks -Djavax.net.ssl.trustStorePassword=changeit AllClient

Os parametros indicados antes do nome da classe são do HTTPS e servem para a VM do Java
