# ECE-BigData-API
API for BigData project.

# How to use
1. Clone the repo
2. Connect to VPN
3. Generate a keytab from EDGE-1 and move into project root folder (same location as `krb5.conf`) and rename it "adaltas.keytab" (replace the old one)
4. Go to `src/main/resources/application.properties` and add your principal (same used by keytab) like the example
5. In terminal : 
   1. `docker build -t BigData-API`
   2. `docker run -p 8080:8080 BigData-API:latest`
6. Access via browser : `http://localhost:8080`, you should see a message like this : "Yay, the server is accessible from docker !"
7. Use Postman to test all the routes :
   1. `/user`
   2. `/channel`
   3. `/message`