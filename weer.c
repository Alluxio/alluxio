/**
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/ioctl.h>
#include <netinet/if_ether.h>
#include <net/if_arp.h>
#include <netinet/udp.h>
#include <netinet/ip.h>
#include <stdio.h>
#include <stdarg.h>
#include <net/if.h>

enum{
    ARP_MSG_SIZE = 0x2a
};

char* strncpy_IFNAMSIZ(char *dst, const char *src)
{
#ifndef IFNAMSIZ
        enum { IFNAMSIZ = 16 };
#endif
            return strncpy(dst, src, IFNAMSIZ);
}

struct arpMsg {
    /* Ethernet header */
    uint8_t h_dest[6]; /* 00 destination ether addr */
    uint8_t h_source[6]; /* 06 source ether addr */
    uint16_t h_proto; /* 0c packet type ID field */

    /* ARP packet */
    uint16_t htype; /* 0e hardware type (must be ARPHRD_ETHER) */
    uint16_t ptype; /* 10 protocol type (must be ETH_P_IP) */
    uint8_t hlen; /* 12 hardware address length (must be 6) */
    uint8_t plen; /* 13 protocol address length (must be 4) */
    uint16_t operation; /* 14 ARP opcode */
    uint8_t sHaddr[6]; /* 16 sender's hardware address */
    uint8_t sInaddr[4]; /* 1c sender's IP address */
    uint8_t tHaddr[6]; /* 20 target's hardware address */
    uint8_t tInaddr[4]; /* 26 target's IP address */
    uint8_t pad[18]; /* 2a pad for min. ethernet payload (60 bytes) */
} PACKED;
const int const_int_1 = 1;
int setsockopt_broadcast(int fd)
{
    return setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &const_int_1, sizeof(const_int_1));
}

char* safe_strncpy(char *dst, const char *src, size_t size)
{
    if (!size) return dst;
    dst[--size] = '\0';
    return strncpy(dst, src, size);
}


int arpping(uint32_t test_ip, uint32_t from_ip, uint8_t *from_mac, const char *interface)
{
    int timeout_ms;
    int s;
    int rv = 1; /* "no reply received" yet */
    struct sockaddr addr; /* for interface name */
    struct arpMsg arp;

    s = socket(PF_PACKET, SOCK_PACKET, htons(ETH_P_ARP));
    if (s == -1) {
        perror("raw_socket");
        return -1;
    }

    if (setsockopt_broadcast(s) == -1) {
        perror("cannot enable bcast on raw socket");
        return -1;
    }

    /* send arp request */
    memset(&arp, 0, sizeof(arp));
    memset(arp.h_dest, 0xff, 6); /* MAC DA */
    memcpy(arp.h_source, from_mac, 6); /* MAC SA */
    arp.h_proto = htons(ETH_P_ARP); /* protocol type (Ethernet) */
    arp.htype = htons(ARPHRD_ETHER); /* hardware type */
    arp.ptype = htons(ETH_P_IP); /* protocol type (ARP message) */
    arp.hlen = 6; /* hardware address length */
    arp.plen = 4; /* protocol address length */
    arp.operation = htons(ARPOP_REQUEST); /* ARP op code */
    memcpy(arp.sHaddr, from_mac, 6); /* source hardware address */
    memcpy(arp.sInaddr, &from_ip, sizeof(from_ip)); /* source IP address */
    /* tHaddr is zero-fiiled */ /* target hardware address */
    memcpy(arp.tInaddr, &test_ip, sizeof(test_ip)); /* target IP address */

    memset(&addr, 0, sizeof(addr));
    safe_strncpy(addr.sa_data, interface, sizeof(addr.sa_data));
    if (sendto(s, &arp, sizeof(arp), 0, &addr, sizeof(addr)) < 0) {
        // TODO: error message? caller didn't expect us to fail,

        // just returning 1 "no reply received" misleads it. 

    }
    close(s);
    return rv;
}

int read_interface(const char *interface, int *ifindex, uint32_t *addr, uint8_t *arp)
{
    int fd;
    struct ifreq ifr;
    struct sockaddr_in *our_ip;

    memset(&ifr, 0, sizeof(ifr));
    fd = socket(AF_INET, SOCK_RAW, IPPROTO_RAW);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy_IFNAMSIZ(ifr.ifr_name, interface);
    if (addr) {
        if (ioctl(fd, SIOCGIFADDR, &ifr) != 0){
            perror("ioctl");
            close(fd);
            return -1;
        }
        our_ip = (struct sockaddr_in *) &ifr.ifr_addr;
        *addr = our_ip->sin_addr.s_addr;
        printf("ip of %s = %s \n", interface, inet_ntoa(our_ip->sin_addr));
    }

    if (ifindex) {
        if (ioctl(fd, SIOCGIFINDEX, &ifr) != 0) {
            close(fd);
            return -1;
        }
        printf("adapter index %d", ifr.ifr_ifindex);
        *ifindex = ifr.ifr_ifindex;
    }

    if (arp) {
        if (ioctl(fd, SIOCGIFHWADDR, &ifr) != 0) {
            close(fd);
            return -1;
        }
        memcpy(arp, ifr.ifr_hwaddr.sa_data, 6);
        printf("adapter hardware address %02x:%02x:%02x:%02x:%02x:%02x\n",
            arp[0], arp[1], arp[2], arp[3], arp[4], arp[5]);
    }
    close(fd);
    return 0;
}

int main(void)
{
    uint32_t TEST_IP = inet_addr("191.192.193.194");
    char interface[]="eth0";
    uint32_t ip;
    uint8_t mac[6];
    read_interface(interface, NULL, &ip, mac); 
    
    while(1){
        arpping(TEST_IP, ip, mac, interface);
        sleep(1);
    }
    return 0;
}
