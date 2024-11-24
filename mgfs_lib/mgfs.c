// IMPLEMENTACIÓN DE LA BIBLIOTECA DE CLIENTE.
// PUEDE USAR EL EJEMPLO DE SOCKETS cliente.c COMO PUNTO DE PARTIDA.
#include <stddef.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/uio.h>
#include "common.h"
#include "common_cln.h"
#include "master.h"
#include "server.h"
#include "mgfs.h"

// TIPOS INTERNOS

// descriptor de un sistema de ficheros:
// tipo interno que almacena información de un sistema de ficheros
typedef struct mgfs_fs {
	int conn_descriptor;
	int def_blocksize;
	int def_rep_factor;
} mgfs_fs;

// descriptor de un fichero:
// tipo interno que almacena información de un fichero
typedef struct mgfs_file {
	int conn_descriptor;
	int blocksize;
	int rep_factor;
	char *fname;
	int bloque_libre;
} mgfs_file;


/* 
 * FASE 1: CREACIÓN DE FICHEROS
 */

// PASO 1: CONEXIÓN Y DESCONEXIÓN

// Establece una conexión con el sistema de ficheros especificado,
// fijando los valores por defecto para el tamaño de bloque y el
// factor de replicación para los ficheros creados en esta sesión.
// Devuelve el descriptor del s. ficheros si OK y NULL en caso de error.
mgfs_fs * mgfs_connect(const char *master_host, const char *master_port,
        int def_blocksize, int def_rep_factor){
	
	int s;
	if ((s=create_socket_cln_by_name(master_host, master_port)) < 0) return NULL;
	
	mgfs_fs* fs = (mgfs_fs*) malloc (sizeof(mgfs_fs));
	/*datos_sesion->master_host = (char*) malloc (strlen(master_host+1));
	datos_sesion->master_host = master_host;
	datos_sesion->master_port = (char*) malloc (strlen(master_port+1));
	datos_sesion->master_port = master_port;*/
	fs->conn_descriptor = s;
	fs->def_blocksize = def_blocksize;
	fs->def_rep_factor = def_rep_factor;
	
	return fs;
}
// Cierra la conexión con ese sistema de ficheros.
// Devuelve 0 si OK y un valor negativo en caso de error.
int mgfs_disconnect(mgfs_fs *fs){
    	if (fs == NULL) return -1;
	close(fs->conn_descriptor); // cierra el socket
	free (fs);
	return 0;
}
// Devuelve tamaño de bloque por defecto y un valor negativo en caso de error.
int mgfs_get_def_blocksize(const mgfs_fs *fs){
    	if (fs == NULL) return -1;
	return fs->def_blocksize;
}
// Devuelve factor de replicación por defecto y valor negativo en caso de error.
int mgfs_get_def_rep_factor(const mgfs_fs *fs){
    	if (fs == NULL) return -1;
	return fs->def_rep_factor;
}

// PASO 2: CREAR FICHERO

// Crea un fichero con los parámetros especificados.
// Si blocksize es 0, usa el valor por defecto.
// Si rep_factor es 0, usa el valor por defecto.
// Devuelve el descriptor del fichero si OK y NULL en caso de error.
mgfs_file * mgfs_create(const mgfs_fs *fs, const char *fname,
        int blocksize, int rep_factor){
        
        if (fs == NULL) return NULL;
        
	mgfs_file* fd = (mgfs_file*) malloc (sizeof(mgfs_file));
	fd->fname = (char*) malloc (strlen(fname)+1);
	fd->fname = strdup (fname);
	if (blocksize == 0) fd->blocksize = fs->def_blocksize;
	else fd->blocksize = blocksize;
	if (rep_factor == 0) fd->rep_factor = fs->def_rep_factor;
	else fd->rep_factor = rep_factor;
	fd->conn_descriptor = fs->conn_descriptor;
	fd->bloque_libre = 0;
	//datos a enviar
	struct iovec iov[5]; // hay que enviar 5 elementos
	int nelem;
	char codigoOp = 'C';
	nelem = 0;
        // preparo el envío del codigo de la operacion
        iov[nelem].iov_base = &codigoOp;
        iov[nelem++].iov_len = sizeof(char);
        
        // preparo el envío del nombre del fichero mandando antes su longitud
        int longitud_fname = strlen(fd->fname);
        int longitud_fname_net = htonl(longitud_fname);
        iov[nelem].iov_base = &longitud_fname_net;
        iov[nelem++].iov_len = sizeof(int);
        iov[nelem].iov_base = fd->fname;
        iov[nelem++].iov_len = longitud_fname;
        
        // preparo el envío del blocksize
        int blocsize_net = htonl(fd->blocksize); 
        iov[nelem].iov_base = &blocsize_net;
        iov[nelem++].iov_len = sizeof(int);
        
      	// preparo el envío del rep_factor
        int rep_factor_net = htonl(fd->rep_factor); 
        iov[nelem].iov_base = &rep_factor_net;
        iov[nelem++].iov_len = sizeof(int);

        // modo de operación de los sockets asegura que si no hay error
        // se enviará todo
        if (writev(fs->conn_descriptor, iov, 5) < 0) {
            perror("error en writev"); return NULL;
        }
        
        int response;
        
    	if (recv(fs->conn_descriptor, &response, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        	perror("error en recv"); return NULL;
    	}
    	response = ntohl(response); //a formato de host
	if (response == -1)
		return NULL;

    return fd;
}
// Cierra un fichero.
// Devuelve 0 si OK y un valor negativo si error.
int mgfs_close(mgfs_file *f){
	if (f == NULL) return -1;
   	free(f);
    	return 0;
}
// Devuelve tamaño de bloque y un valor negativo en caso de error.
int mgfs_get_blocksize(const mgfs_file *f){
   	 if (f == NULL) return -1;
   	 return f->blocksize;
}
// Devuelve factor de replicación y valor negativo en caso de error.
int mgfs_get_rep_factor(const mgfs_file *f){
	if (f == NULL) return -1;
    	return f->rep_factor;
}

// Devuelve el nº ficheros existentes y un valor negativo si error.
int _mgfs_nfiles(const mgfs_fs *fs) {
	
	char codigoOp = 'N';
	if (write(fs->conn_descriptor, &codigoOp, sizeof(int))<0) {
        	perror("error en write"); return -1;
	}
	
	int response;
    	if (recv(fs->conn_descriptor, &response, sizeof(int), MSG_WAITALL) != sizeof(int)) {
		perror("error en recv"); return -1;
    	}
    	response = ntohl(response); //a formato de host
    	
    return response;
    
}

// PASO 3: APERTURA DE FICHERO PARA LECTURA

// Abre un fichero para su lectura.
// Devuelve el descriptor del fichero si OK y NULL en caso de error.
mgfs_file * mgfs_open(const mgfs_fs *fs, const char *fname){
	int nelem = 0;
	char codigoOp = 'O';
	struct iovec iov[3]; // hay que enviar 3 elementos
        // preparo el envío del codigo de la operacion
        iov[nelem].iov_base = &codigoOp;
        iov[nelem++].iov_len = sizeof(char);
        
        // preparo el envío del nombre del fichero mandando antes su longitud
        int longitud_fname = strlen(fname);
        int longitud_fname_net = htonl(longitud_fname);
        iov[nelem].iov_base = &longitud_fname_net;
        iov[nelem++].iov_len = sizeof(int);
        iov[nelem].iov_base = (char*)fname;
        iov[nelem++].iov_len = longitud_fname;
	
        if (writev(fs->conn_descriptor, iov, 3) < 0) {
            perror("error en writev"); return NULL;
        }
        
        int err;
        mgfs_file* fd = (mgfs_file*) malloc (sizeof(mgfs_file));
        
        if (recv(fs->conn_descriptor, &err, sizeof(int), MSG_WAITALL) != sizeof(int)) {
		perror("error en recv 1"); return NULL;
    	}

    	if (ntohl(err) == -1) return NULL;
    	
    	int blocksize;
    	if (recv(fs->conn_descriptor, &blocksize, sizeof(int), MSG_WAITALL) != sizeof(int)) {
		perror("error en recv 2"); return NULL;
    	}
    	fd->blocksize = ntohl(blocksize);
    	
    	int rep_factor;
    	if (recv(fs->conn_descriptor, &rep_factor, sizeof(int), MSG_WAITALL) != sizeof(int)) {
		perror("error en recv 3"); return NULL;
    	}
    	fd->rep_factor = ntohl(rep_factor);
    	
    	fd->fname = strdup (fname);
   	
   	return fd;
}

/* 
 * FASE 2: ALTA DE LOS SERVIDORES
 */

// Operación interna para test; no para uso de las aplicaciones.
// Obtiene la información de localización (ip y puerto en formato de red)
// de un servidor.
// Devuelve 0 si OK y un valor negativo si error.
int _mgfs_serv_info(const mgfs_fs *fs, int n_server, unsigned int *ip,
        unsigned short *port){
	
        struct iovec iov[2]; // hay que enviar 2 elementos
	int nelem = 0;
	char codigoOp = 'S';
        // preparo el envío del codigo de la operacion
        iov[nelem].iov_base = &codigoOp;
        iov[nelem++].iov_len = sizeof(char);

        // preparo el envío del numero del servidor
        int n_server_net = htonl(n_server);
        iov[nelem].iov_base = &n_server_net;
        iov[nelem++].iov_len = sizeof(int);
        
        if (writev(fs->conn_descriptor, iov, 2) < 0) {
            perror("error en writev"); return -1;
        }
        
        int err = 0;
	if (recv(fs->conn_descriptor, &err, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
       	err = ntohl (err);
       	if (err == -1) return err;
      
        unsigned int ip_net;
	if (recv(fs->conn_descriptor, &ip_net, sizeof(unsigned int), MSG_WAITALL) != sizeof(unsigned int)) err = -1;
	*ip = ip_net;
	
	unsigned short puerto_net;
	if (recv(fs->conn_descriptor, &puerto_net, sizeof(unsigned short), MSG_WAITALL) != sizeof(unsigned short)) err = -1;
	*port = puerto_net;
	
    return err;
}

/* 
 * FASE 3: ASIGNACIÓN DE SERVIDORES A BLOQUES.
 */

// Operación interna: será usada por write.
// Asigna servidores a las réplicas del siguiente bloque del fichero.
// Devuelve la información de localización (ip y puerto en formato de red)
// de cada una de ellas.
// Retorna 0 si OK y un valor negativo si error.
int _mgfs_alloc_next_block(const mgfs_file *file, unsigned int *ips, unsigned short *ports) {
    	struct iovec iov[3]; // hay que enviar 3 elementos
	int nelem = 0;
	char codigoOp = 'A';
        // preparo el envío del codigo de la operacion
        iov[nelem].iov_base = &codigoOp;
        iov[nelem++].iov_len = sizeof(char);
        
        // preparo el envío del nombre del fichero mandando antes su longuitud
        int fname_len = strlen(file->fname);
        int fname_len_net = htonl(fname_len);
        iov[nelem].iov_base = &fname_len_net;
        iov[nelem++].iov_len = sizeof(int);
    	// envio el nombre del fichero
    	iov[nelem].iov_base = file->fname;
        iov[nelem++].iov_len = fname_len;
        
        if (writev(file->conn_descriptor, iov, 3) < 0) {
            perror("error en writev"); return -1;
        }
        
 	int err = 0;
	if (recv(file->conn_descriptor, &err, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
       	err = ntohl (err);
       	if (err == -1) return err;
      
	if (recv(file->conn_descriptor, ips, sizeof(unsigned int) * file->rep_factor, MSG_WAITALL) !=  sizeof(unsigned int) * file->rep_factor) err = -1;

	if (recv(file->conn_descriptor, ports, sizeof(unsigned short) * file->rep_factor, MSG_WAITALL) != sizeof(unsigned short) * file->rep_factor) err = -1;
	
    return err;
}

// Obtiene la información de localización (ip y puerto en formato de red)
// de los servidores asignados a las réplicas del bloque.
// Retorna 0 si OK y un valor negativo si error.
int _mgfs_get_block_allocation(const mgfs_file *file, int n_bloque,
        unsigned int *ips, unsigned short *ports) {
    	struct iovec iov[4]; // hay que enviar 4 elementos
	int nelem = 0;
	char codigoOp = 'I';
        // preparo el envío del codigo de la operacion
        iov[nelem].iov_base = &codigoOp;
        iov[nelem++].iov_len = sizeof(char);
        
        // preparo el envío del nombre del fichero mandando antes su longuitud
        int fname_len = strlen(file->fname);
        int fname_len_net = htonl(fname_len);
        iov[nelem].iov_base = &fname_len_net;
        iov[nelem++].iov_len = sizeof(int);
    	//envio el nombre del fichero
    	iov[nelem].iov_base = file->fname;
        iov[nelem++].iov_len = fname_len;
    	//envio el numero de bloque
 	int n_bloque_net = htonl (n_bloque);
    	iov[nelem].iov_base = &n_bloque_net;
        iov[nelem++].iov_len = sizeof(int);
        
        if (writev(file->conn_descriptor, iov, 4) < 0) {
            perror("error en writev"); return -1;
        }
        
	int err = 0;
	if (recv(file->conn_descriptor, &err, sizeof(int), MSG_WAITALL) != sizeof(int)) err = -1;
       	err = ntohl (err);
       	if (err == -1) return err;
      

	if (recv(file->conn_descriptor, ips, sizeof(unsigned int) * file->rep_factor, MSG_WAITALL) != sizeof(unsigned int) * file->rep_factor) err = -1;

	if (recv(file->conn_descriptor, ports, sizeof(unsigned short) * file->rep_factor, MSG_WAITALL) != sizeof(unsigned short) * file->rep_factor) err = -1;

        
    return err;
}


/*
 * FASE 4: ESCRITURA EN EL FICHERO.
 */

// Escritura en el fichero.
// Devuelve el tamaño escrito si OK y un valor negativo si error.
// Por restricciones de la práctica, "size" tiene que ser múltiplo
// del tamaño de bloque y el valor devuelto deber ser igual a "size".
int mgfs_write(mgfs_file *file, const void *buff, unsigned long size){
	if (size % mgfs_get_blocksize(file)) return -1;
    	unsigned int* ips;
    	unsigned short* ports; 
    	int s, n_bloque, n_bloques, bytes_escritos, total_bytes_escritos, n_replica;
    	
    	n_bloques = size / mgfs_get_blocksize(file);
    	total_bytes_escritos = 0;
    	for (int i=0; i<n_bloques; i++) {
	    	n_bloque = file->bloque_libre;
		file->bloque_libre = n_bloque + 1;
	 	ips = (unsigned int*) malloc (file->rep_factor*sizeof(unsigned int));
 		ports = (unsigned short*) malloc (file->rep_factor*sizeof(unsigned short));
	 	if (_mgfs_alloc_next_block(file, ips, ports) == -1) return -1;
	  
		s = create_socket_cln_by_addr(ips[0], ports[0]);
		struct iovec iov[9]; // hay que enviar 9 elementos
		int nelem = 0;
		
		// preparo el envío del nombre del fichero mandando antes su longuitud
		int fname_len = strlen(file->fname);
		int fname_len_net = htonl(fname_len);
		iov[nelem].iov_base = &fname_len_net;
		iov[nelem++].iov_len = sizeof(int);
	    	//envio el nombre del fichero
	    	iov[nelem].iov_base = file->fname;
		iov[nelem++].iov_len = fname_len;
	    	//envio el numero de bloque
	 	n_bloque = htonl (n_bloque);
	    	iov[nelem].iov_base = &n_bloque;
		iov[nelem++].iov_len = sizeof(int);
		//envio el numero de replica
	 	n_replica = htonl (0);
	    	iov[nelem].iov_base = &n_replica;
		iov[nelem++].iov_len = sizeof(int);
		//envio el contenido mandando antes su longuitud
		unsigned long size_net = htonl(mgfs_get_blocksize(file));
		iov[nelem].iov_base = &size_net;
		iov[nelem++].iov_len = sizeof(unsigned long);
	    	//envio el contenido
	    	iov[nelem].iov_base = (void*) (buff + total_bytes_escritos);
		iov[nelem++].iov_len = mgfs_get_blocksize(file);
		//envio el factor de replicacion
		int rep_factor_net = htonl (file->rep_factor);
		iov[nelem].iov_base = &rep_factor_net;
		iov[nelem++].iov_len = sizeof(int);
		//envio las ips del resto de servidores
		iov[nelem].iov_base = ips;
		iov[nelem++].iov_len = file->rep_factor*sizeof(unsigned int);
		//envio los puertos del resto de servidores
		iov[nelem].iov_base = ports;
		iov[nelem++].iov_len = file->rep_factor*sizeof(unsigned short);
		
		if (writev(s, iov, 9) < 0) {
		    perror("error en writev"); return -1;
		}
		
		if (recv(s, &bytes_escritos, sizeof(int), MSG_WAITALL) != sizeof(int)) return -1;
		bytes_escritos = ntohl (bytes_escritos);
		total_bytes_escritos += bytes_escritos;
   	}
   	
    return total_bytes_escritos;
}

