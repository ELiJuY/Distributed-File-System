// PUEDE INCLUIR AQUÍ LA DEFINICIÓN DE FUNCIONALIDADES COMUNES
// RELACIONADAS CON UN SERVIDOR DE SOCKETS (EL MAESTRO Y LOS SERVIDORES
// ACTÚAN DE SERVIDORES).
//
// INICIALMENTE, INCLUYE LA DEFINICIÓN DE UNA FUNCIÓN QUE CREA UN SOCKET DE
// SERVIDOR, QUE PUEDE USAR SI LO CONSIDERA OPORTUNO.
//
// PUEDE MODIFICAR EL FICHERO A DISCRECIÓN.

#ifndef _COMMON_SRV_H
#define _COMMON_SRV_H        1

int create_socket_srv(unsigned short pto_pedido, unsigned short *pto_asignado);

#endif // _COMMON_SRV_H