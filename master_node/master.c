#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include "master.h"
#include "common.h"
#include "common_srv.h"
#include "map.h"
#include "array.h"
#include <netinet/in.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <pthread.h>

// asigna servidores siguiendo un turno rotatorio
static int alloc_srv(array *lista_srv){
    static int next_srv=0;
    int lista_size = array_size(lista_srv);
    if (lista_size==0) return -1; // no hay servidores
    return __sync_fetch_and_add(&next_srv,1) % lista_size; // op. atómica
}

// información que se la pasa el thread creado
typedef struct thread_info {
    int socket;
    unsigned int ip;
    array* array_servs;
    map* map_files;
} thread_info;

typedef struct master_file {
	int blocksize;
	int rep_factor;
	char *fname;
 	array* file_blocks;
} master_file;

typedef struct serv_info {
	unsigned int ip;
	unsigned short puerto;
} serv_info;


// función del thread
void *servicio(void *arg){
	char codigoOp;
	thread_info *thinf = arg; // argumento recibido

	while (1) {

		// cada "petición" comienza con un char que indica el tipo de operacion
		if (recv(thinf->socket, &codigoOp, sizeof(char), MSG_WAITALL) != sizeof(char)) return NULL;
	
		int err = 0;
		int tam_fname, rep_factor;
		master_file* mf;
		serv_info* server_info;
		char* name;
		int n_serv;
		
		switch (codigoOp) {

			case 'C':
				;
				//Crea un nuevo fichero
				mf = (master_file*) malloc (sizeof(master_file));
				
				//Recibe el tamaño del nombre del fichero y reserva espacio para el
				if (recv(thinf->socket, &tam_fname, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				tam_fname = ntohl (tam_fname);
				mf->fname = (char*) malloc (tam_fname+1);
				
				//Recibe el nombre del fichero
				if (recv(thinf->socket, mf->fname, tam_fname, MSG_WAITALL) != tam_fname) err = -1;
				mf->fname[tam_fname]='\0'; // añadimos el carácter nulo
				int tam_blocksize;
				
				//Recibe el blocksize del fichero
				if (recv(thinf->socket, &tam_blocksize, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				mf->blocksize = ntohl (tam_blocksize);
				int tam_rep_factor;
				
				//Recibe el rep_factor del fichero
				if (recv(thinf->socket, &tam_rep_factor, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				mf->rep_factor = ntohl(tam_rep_factor);
				mf->file_blocks = array_create(1);
				
				//Se añade al mapa de files
				if (map_put(thinf->map_files, mf->fname, mf) < 0) err = -1;
				
				//Devuelve si la operacion ha sido exitosa o no
				err = htonl(err); //a formato de red
				if (write(thinf->socket, &err, sizeof(int)) < 0) {
   					perror("error en write"); 
				}
				
				break;
			case 'N':
				;
				//Envia el numero de ficheros en el sistema de ficheros
				int n_files = map_size(thinf->map_files);
				n_files = htonl(n_files); //a formato de red
				if (write(thinf->socket, &n_files, sizeof(int)) < 0) {
   					perror("error en write");
				}
				
				break;
				
			case 'O':
				;
				//Abre un fichero para lectura
				
				//Recibe el tamaño del nombre del fichero y reserva espacio para el
				if (recv(thinf->socket, &tam_fname, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				int tam_fname_host = ntohl (tam_fname);
				name = (char*) malloc (tam_fname_host+1);
				
				//Recibe el nombre del fichero y obtiene su informacion del mapa de ficheros
				if (recv(thinf->socket, name, tam_fname_host, MSG_WAITALL) != tam_fname_host) err = -1;
				name[tam_fname_host] = '\0'; // añadimos el carácter terminador
				mf = map_get (thinf->map_files, name, &err);
				
				//Si no existe devuelve un error
				if (err == -1) {
					if (write(thinf->socket, &err, sizeof(int)) < 0) {
   						perror("error en write");
					}
				}
				else {
					struct iovec iov[3]; // hay que enviar 3 elementos
					int nelem = 0;
					// preparo el envío del resultado de la operacion
					int err_net = htonl(err);
					iov[nelem].iov_base = &err_net;
					iov[nelem++].iov_len = sizeof(int);
				
					// preparo el envío del blocksize
					int blocksize = mf->blocksize;
					blocksize = htonl(blocksize);
					iov[nelem].iov_base = &blocksize;
					iov[nelem++].iov_len = sizeof(int);
					// preparo el envío del rep_factor
					int rep_factor = mf->rep_factor;
					rep_factor = htonl(rep_factor);
					iov[nelem].iov_base = &rep_factor;
					iov[nelem++].iov_len = sizeof(int);
					
					if (writev(thinf->socket, iov, 3) < 0) {
						perror("error en writev");
	       				}
				}
				
				free (name);
       				
       				break;
			case 'V':
       				;
       				//Da de alta un servidor en el sistema de ficheros
       				unsigned short puerto;
       				server_info = (serv_info*) malloc (sizeof(serv_info));
				
				//Recibe el puerto por el que escucha el servidor
				if (recv(thinf->socket, &puerto, sizeof(unsigned short), MSG_WAITALL) != sizeof(unsigned short)) perror("error en recv");;
				server_info->puerto = ntohs (puerto);
				server_info->ip = thinf->ip;
				array_append(thinf->array_servs, server_info);
       				break;
       			case 'S':
       				;
       				//Envia los datos de un servidor dado de alta en el sistema de ficheros (direccion ip y puerto de escucha)
				
				//Recibe el numero de servidor
				if (recv(thinf->socket, &n_serv, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				n_serv = ntohl(n_serv);
				server_info = array_get(thinf->array_servs, n_serv, &err);
				
				//Si el servidor no existe devuelve un error
				if (err == -1) {
					err = htonl (err);
					if (write(thinf->socket, &err, sizeof(int)) < 0) {
   						perror("error en write");
					}
				}
				else {
					struct iovec iov[3]; // hay que enviar 3 elementos
					int nelem = 0;
					// preparo el envío del resultado de la operacion
					int err_net = htonl(err);
					iov[nelem].iov_base = &err_net;
					iov[nelem++].iov_len = sizeof(int);
				
					// preparo el envío de la ip
        				unsigned int ip = server_info->ip;
					iov[nelem].iov_base = &ip;
					iov[nelem++].iov_len = sizeof(unsigned int);
					
					// preparo el envío del puerto
					unsigned short puerto = server_info->puerto;
					puerto = htons(puerto);
					iov[nelem].iov_base = &puerto;
					iov[nelem++].iov_len = sizeof(unsigned short);
					
					if (writev(thinf->socket, iov, 3) < 0) {
						perror("error en writev");
	       				}
				}
				
       				break;
       			case 'A': 
       				;
       				// Asigna un servidor a la réplica del siguiente bloque del un fichero
				
				// Recibe el tamaño del nombre del fichero y reserva espacio para el
				if (recv(thinf->socket, &tam_fname, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				tam_fname = ntohl (tam_fname);
       				name = (char*) malloc (tam_fname+1);
				
				// Recibe el nombre del fichero y obtiene su informacion del mapa de ficheros
				if (recv(thinf->socket, name, tam_fname, MSG_WAITALL) != tam_fname) err = -1;
				name[tam_fname] = '\0';
				mf = map_get (thinf->map_files, name, &err);
				
				rep_factor = mf->rep_factor;
				
				//Si el fichero no existe devuelve error
				if (err == -1) {
					err = htonl (err);
					if (write(thinf->socket, &err, sizeof(int)) < 0) {
   						perror("error en write");
					}
				}
				else {
					serv_info** servers_info = (serv_info**) malloc (rep_factor*sizeof(serv_info*));
					unsigned int* ips = (unsigned int*) malloc (rep_factor*sizeof(unsigned int));
					unsigned short* ports = (unsigned short*) malloc (rep_factor*sizeof(unsigned short));

					for (int i = 0; i<rep_factor; i++) {
						//Asigna un servidor para cada replica del nuevo bloque
						n_serv = alloc_srv(thinf->array_servs);
						servers_info[i] = array_get(thinf->array_servs, n_serv, &err);
						ips[i] = servers_info[i]->ip;
						ports[i] = htons(servers_info[i]->puerto);
					}
					array_append(mf->file_blocks, servers_info);
					struct iovec iov[3]; // hay que enviar 3 elementos
					int nelem = 0;
					// preparo el envío del resultado de la operacion
					int err_net = htonl(err);
					iov[nelem].iov_base = &err_net;
					iov[nelem++].iov_len = sizeof(int);

					// preparo el envío de la ip del servidor asignado
					iov[nelem].iov_base = ips;
					iov[nelem++].iov_len = sizeof(unsigned int) * rep_factor;
					
					// preparo el envío del puerto del servidor asignado
					iov[nelem].iov_base = ports;
					iov[nelem++].iov_len = sizeof(unsigned short) * rep_factor;
					
					if (writev(thinf->socket, iov, 3) < 0) {
						perror("error en writev");
	       				}
				}
				
				free(name);
				
				break;
			case 'I':
				//Obtiene la ip y puerto de los servidores asignados a las réplicas de un bloque de un fichero
				
				//Recibe el tamaño del nombre del fichero y reserva espacio para el
				if (recv(thinf->socket, &tam_fname, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				tam_fname = ntohl (tam_fname);
       				name = (char*) malloc (tam_fname+1);
				
				//Recibe el nombre del fichero y obtiene su informacion del mapa de ficheros
				if (recv(thinf->socket, name, tam_fname, MSG_WAITALL) != tam_fname) err = -1;
				name[tam_fname] = '\0';
				mf = map_get (thinf->map_files, name, &err);
				
				//Recibe el numero de bloque y busca el servidor asignado al mismo
				int n_bloque;
				if (recv(thinf->socket, &n_bloque, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
				n_bloque = ntohl (n_bloque);
				
				//Rep factor del fichero
				rep_factor = mf->rep_factor;
				
				serv_info** servers_info = array_get(mf->file_blocks, n_bloque, &err);
				
				//Si el numero de bloque no tiene un servidor asignado devuelve error
				if (err == -1) {
					err = htonl (err);
					if (write(thinf->socket, &err, sizeof(int)) < 0) {
   						perror("error en write");
					}
				}
				
				else {
					unsigned int* ips = (unsigned int*) malloc (rep_factor*sizeof(unsigned int));
					unsigned short* ports = (unsigned short*) malloc (rep_factor*sizeof(unsigned short));
				
					for (int i = 0; i<rep_factor; i++) {
						ips[i] = servers_info[i]->ip;
						ports[i] = htons (servers_info[i]->puerto);
					}
				
					struct iovec iov[3]; // hay que enviar 3 elementos
					int nelem = 0;
					// preparo el envío del resultado de la operacion
					int err_net = htonl(err);
					iov[nelem].iov_base = &err_net;
					iov[nelem++].iov_len = sizeof(int);
					
					// preparo el envío de la ip
					iov[nelem].iov_base = ips;
					iov[nelem++].iov_len = rep_factor*sizeof(unsigned int);
					
					// preparo el envío del puerto
					iov[nelem].iov_base = ports;
					iov[nelem++].iov_len = rep_factor*sizeof(unsigned short);
					
					if (writev(thinf->socket, iov, 3) < 0) {
						perror("error en writev");
	       				}
				}
				
				free(name);
				
				break;
		}		
	}
	close(thinf->socket);
	free(thinf);
	return NULL;
}

int main(int argc, char *argv[]) {
    
	int s, s_conec;
	unsigned int tam_dir;
	struct sockaddr_in dir_cliente;
    	map *files_info;
    	array *servers_info;
	// inicializa el mapa de los metadatos de los archivos y el array de servidores
	files_info = map_create(key_string, 1);
 	servers_info = array_create(1);
    	    
	if (argc!=2) {
	fprintf(stderr, "Uso: %s puerto\n", argv[0]);
	return -1;
	}

	// inicializa el socket y lo prepara para aceptar conexiones
	if ((s=create_socket_srv(atoi(argv[1]), NULL)) < 0) return -1;

	// prepara atributos adecuados para crear thread "detached"
	pthread_t thid;
	pthread_attr_t atrib_th;
	pthread_attr_init(&atrib_th); // evita pthread_join
	pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);
	
	while(1) {
		tam_dir=sizeof(dir_cliente);
		// acepta la conexión
		if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0){
		    perror("error en accept");
		    close(s);
		    return -1;
		}
		

		printf("conectado cliente con ip %u y puerto %u (formato red)\n",
			dir_cliente.sin_addr.s_addr, dir_cliente.sin_port);
		
		// crea el thread de servicio
		thread_info *thinf = malloc(sizeof(thread_info));
		thinf->socket = s_conec;
		thinf->ip = dir_cliente.sin_addr.s_addr;
		thinf->map_files = files_info;
		thinf->array_servs = servers_info;
		pthread_create(&thid, &atrib_th, servicio, thinf);
	}
	close(s);
	return 0;
}
