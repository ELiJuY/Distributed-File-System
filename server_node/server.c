// DADO QUE ESTE PROGRAMA ACTÚA COMO CLIENTE Y SERVIDOR, PUEDE USAR LOS EJEMPLOS DE SOCKETS cliente.c y servidor.c COMO PUNTO DE PARTIDA.
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include "server.h"
#include "master.h"
#include "common.h"
#include "common_srv.h"
#include "common_cln.h"

// información que se la pasa el thread creado
typedef struct thread_info {
    int socket;
    char* directorio;
} thread_info;

// función del thread
void *servicio(void *arg){

	thread_info *thinf = arg; // argumento recibido
	char* fname;
	int fname_len, n_bloque, f, n_replica;
	void *p;
	unsigned long buffer_len;

	// Guarda el directorio actual
	char *initial_dir = getcwd(NULL,0);

	//Recibe la longuitud del nombre de fichero y reserva espacio
	if (recv(thinf->socket, &fname_len, sizeof(int), MSG_WAITALL) != sizeof(int)) { close(thinf->socket); return NULL; }
	fname_len = ntohl (fname_len);
	fname = (char*) malloc (fname_len + 1);
	
	//Recibe el nombre del fichero
	if (recv(thinf->socket, fname, fname_len, MSG_WAITALL) != fname_len) { close(thinf->socket); return NULL; }
	fname[fname_len] = '\0';	
	
	//Recibe el nº de bloque
	if (recv(thinf->socket, &n_bloque, sizeof(int), MSG_WAITALL) != sizeof(int)) { close(thinf->socket); return NULL; }
	n_bloque = ntohl(n_bloque);	
	
	//Recibe el nº de replica
	if (recv(thinf->socket, &n_replica, sizeof(int), MSG_WAITALL) != sizeof(int)) { close(thinf->socket); return NULL; }
	n_replica = ntohl(n_replica);
	
	//Recibe la longuitud del contenido del fichero
	if (recv(thinf->socket, &buffer_len, sizeof(unsigned long), MSG_WAITALL) != sizeof(unsigned long)) { close(thinf->socket); return NULL; }
	buffer_len = ntohl(buffer_len);	
	
	//Cambia a su carpeta de almacenamiento y crea la carpeta de los bloques del fichero si no existia
	chdir(thinf->directorio);
	if (chdir(fname) == -1) {
		mkdir(fname, 0755); 
		chdir(fname);
	}

	//Nombre del archivo que guarda el bloque del fichero
	char nombre_file [12];
 	snprintf(nombre_file, sizeof(nombre_file), "%d%c%d", n_bloque, '_', n_replica);
	
	//Crea el fichero
	if ((f=open(nombre_file, O_CREAT|O_TRUNC|O_RDWR, 0666)) < 0) {
		perror("error en creat"); close(thinf->socket); return NULL;
   	}
    	if (ftruncate(f,buffer_len) < 0) {
        	perror("error en ftruncate");close(thinf->socket); return NULL;
    	}
	if ((p = mmap(NULL, buffer_len, PROT_WRITE, MAP_SHARED, f, 0)) == MAP_FAILED) {
        	perror("error en mmap"); close(thinf->socket); close(f); return NULL;
    	}
    	
    	close(f);
    	
	//Recibe el contenido del fichero
	if (recv(thinf->socket, p, buffer_len, MSG_WAITALL) != buffer_len) { close(thinf->socket); return NULL; }
	
	//Si es la copia primaria envia el contenido a las secundarias
	if (n_replica == 0) {
	
		int rep_factor;
		if (recv(thinf->socket, &rep_factor, sizeof(int), MSG_WAITALL) != sizeof(int)) return NULL;
		rep_factor = ntohl (rep_factor);
		
		unsigned int* ips = (unsigned int*) malloc (rep_factor*sizeof(unsigned int));
		if (recv(thinf->socket, ips, rep_factor*sizeof(unsigned int), MSG_WAITALL) != rep_factor*sizeof(unsigned int)) return NULL;
		
		unsigned short* ports = (unsigned short*) malloc (rep_factor*sizeof(unsigned short));
		if (recv(thinf->socket, ports, rep_factor*sizeof(unsigned short), MSG_WAITALL) != rep_factor*sizeof(unsigned short)) return NULL;
		
		for (int i = 1; i < rep_factor; i++) {
			chdir(initial_dir);
			n_replica++;
			int s = create_socket_cln_by_addr(ips[i], ports[i]);
			struct iovec iov[6]; // hay que enviar 6 elementos
			int nelem = 0;
			
			// preparo el envío del nombre del fichero mandando antes su longuitud
			int fname_len_net = htonl(fname_len);
			iov[nelem].iov_base = &fname_len_net;
			iov[nelem++].iov_len = sizeof(int);
		    	//envio el nombre del fichero
		    	iov[nelem].iov_base = fname;
			iov[nelem++].iov_len = fname_len;
		    	//envio el numero de bloque
		 	n_bloque = htonl (n_bloque);
		    	iov[nelem].iov_base = &n_bloque;
			iov[nelem++].iov_len = sizeof(int);
			//envio el numero de replica
		 	n_replica = htonl (n_replica);
		    	iov[nelem].iov_base = &n_replica;
			iov[nelem++].iov_len = sizeof(int);
			//envio el contenido mandando antes su longuitud
			unsigned long size_net = htonl(buffer_len);
			iov[nelem].iov_base = &size_net;
			iov[nelem++].iov_len = sizeof(unsigned long);
		    	//envio el contenido
		    	iov[nelem].iov_base = p;
			iov[nelem++].iov_len = buffer_len;

			if (writev(s, iov, 6) < 0) {
			    perror("error en writev"); return NULL;
			}
			
		 	n_bloque = ntohl (n_bloque);
		 	n_replica = ntohl (n_replica);
		 	
		 	int res;
			if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) return NULL;


		}
	}
	
	//Si no ha habido ningun error devuelve en nº de bytes escritos
	free(fname);
   	munmap(p, buffer_len);
	buffer_len = htonl(buffer_len);	
	buffer_len = (int) buffer_len;
	//Vuelve al directorio inicial
	chdir(initial_dir);
	free (initial_dir);
	printf("conexión del cliente cerrada\n");
	
	if (write(thinf->socket, &buffer_len, sizeof(int)) < 0) {
		perror("error en write");	
	}
	
	free(thinf);
	close(thinf->socket);
	return NULL;
}

int main(int argc, char *argv[]) {
	if (argc!=4) {
	fprintf(stderr, "Uso: %s nombre_dir master_host master_puerto\n", argv[0]);
	return -1;
	}
	// Asegurándose de que el directorio de almacenamiento existe
	mkdir(argv[1], 0755);

	int s, s_conec, m;
	unsigned int tam_dir;
	struct sockaddr_in dir_cliente;
	unsigned short puerto;
	
	// inicializa el socket y lo prepara para aceptar conexiones
	if ((s=create_socket_srv(0, &puerto)) < 0) return -1;
	
	// prepara atributos adecuados para crear thread "detached"
	pthread_t thid;
	pthread_attr_t atrib_th;
	pthread_attr_init(&atrib_th); // evita pthread_join
	pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);
	
	//conecta con el maestro
	if ((m=create_socket_cln_by_name(argv[2], argv[3])) < 0) return -1;
	
	char codigoOp = 'V';
	struct iovec iov[2]; // hay que enviar 2 elementos
	int nelem = 0;
	
	// preparo el envío del codigo de la operacion
	iov[nelem].iov_base = &codigoOp;
	iov[nelem++].iov_len = sizeof(char);
	// preparo el envío del puerto (que ya esta en formato de red)
	iov[nelem].iov_base = &puerto;
	iov[nelem++].iov_len = sizeof(unsigned short);

        if (writev(m, iov, 2) < 0) {
            perror("error en writev"); return -1;
        }
        
        close(m);
        
        while (1) {
        	tam_dir=sizeof(dir_cliente);
        	
        	// acepta la conexión
        	if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir)) < 0){
			perror("error en accept");
          	  	close(s);
            		return -1;
        	}
        	printf("conectado cliente con ip %u y puerto %u (formato red)\n",
                dir_cliente.sin_addr.s_addr, dir_cliente.sin_port);
        	
        	// crea el thread de servicio
		thread_info *thinf = malloc(sizeof(thread_info));
		thinf->socket = s_conec;
		thinf->directorio = (char*) malloc (strlen(argv[1])+1);
		thinf->directorio = argv[1];
		thinf->directorio[strlen(argv[1])] = '\0';
		pthread_create(&thid, &atrib_th, servicio, thinf);
        }

    return 0;
}

