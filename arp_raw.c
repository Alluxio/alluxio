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
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include "struct.h"
#define BUFFER_MAX 2048

void analyseICMP(ICMP_DATA *icmp)
{
	printf("type:%u\n",icmp->type);
	printf("code:%u\n",icmp->code);
	printf("checksum%u\n:",icmp->checksum);
}
void analyseUDP(UDP_HEADER *udp)
{
	printf("source port:%u\n",udp->sport);
	printf("dest port:%u\n",udp->dport);
	printf("len:%u\n",udp->len);
	printf("sum:%u\n",udp->sum);
}
void analyseTCP(TCP_HEADER *tcp)
{
	printf("source port:%u\n",tcp->sport);
	printf("dest port:%u\n",tcp->dport);
	printf("seq:%u\n",tcp->seq);
	printf("ack:%u\n",tcp->ack);
	printf("lenres:%u\n",tcp->lenres);
	printf("flag:%u\n",tcp->flag);
	printf("win:%u\n",tcp->win);
	printf("sum:%u\n",tcp->sum);
	printf("urp:%u\n",tcp->urp);
}
void ipdata(char *buffer)
{
	IP_HEADER *ip;
	ip=(IP_HEADER *)(buffer+14);
	size_t iplen = (ip->ver_hlen&0x0f)*4;
	printf("version:%d\n",ip->ver_hlen>>4);
	printf("head_length:%d\n",ip->ver_hlen&0xf);
	printf("tos:%d\n",ip->tos);
	printf("data_length:%d\n",ip->data_length);
	printf("ident:%d\n",ip->ident);
	printf("flags:%d\n",ip->flags);
	printf("ttl:%d\n",ip->ttl);
	printf("proto:%d\n",ip->proto);
	printf("checksum:%d\n",ip->checksum);
	printf("sourceip:%d.%d.%d.%d\n",ip->sourceip[0],ip->sourceip[1],ip->sourceip[2],ip->sourceip[3]);
	printf("destip:%d.%d.%d.%d\n",ip->destip[0],ip->destip[1],ip->destip[2],ip->destip[3]);
	printf("Protocol:");
	switch(ip->proto)
	{
		case IPPROTO_ICMP:printf("icmp\n");
				  ICMP_DATA *icmp=(ICMP_DATA *)(buffer+14+iplen);
				  analyseICMP(icmp);
				  break;
		case IPPROTO_IGMP:printf("igmp\n");
				  break;
		case IPPROTO_IPIP:printf("ipip\n");
				  break;
		case IPPROTO_TCP:printf("tcp\n");
				 TCP_HEADER *tcp=(TCP_HEADER *)(buffer+14+iplen);
				 analyseTCP(tcp);
				 break;
		case IPPROTO_UDP:printf("udp\n");
				 UDP_HEADER *udp=(UDP_HEADER *)(buffer+14+iplen);
				 analyseUDP(udp);
				 break;
		default:printf("Pls query yourself\n");
	}
}

void arp_data(char *buffer)
{
	ARP_RARP *ap;
	ap=(ARP_RARP *)(buffer+14);
	printf("ar_hrd:%d\n",ap->ar_hrd);
	printf("ar_pro:%d\n",ap->ar_pro);
	printf("ar_hln:%d\n",ap->ar_hln);
	printf("ar_pln:%d\n",ap->ar_pln);
	printf("optype:%d\n",ap->optype);
	printf("%d:%d:%d:%d:%d:%d ==> %d:%d:%d:%d:%d:%d\n",ap->arp_sha[0],ap->arp_sha[1],ap->arp_sha[2],ap->arp_sha[3],ap->arp_sha[4],ap->arp_sha[5],ap->arp_tha[0],ap->arp_tha[1],ap->arp_tha[2],ap->arp_tha[3],ap->arp_tha[4],ap->arp_tha[5]);
	printf("%d.%d.%d.%d ==> %d.%d.%d.%d\n",ap->arp_spa[0],ap->arp_spa[1],ap->arp_spa[2],ap->arp_spa[3],ap->arp_tpa[0],ap->arp_tpa[1],ap->arp_tpa[2],ap->arp_tpa[3]);
}



int main(int argc,char* argv[])
{
	int sock_fd;
	int n_read;
	char buffer[BUFFER_MAX];
	MAC_HEAD *machead;
	
	if((sock_fd=socket(PF_PACKET,SOCK_RAW,htons(ETH_P_ARP)))<0)
	{
		printf("error create raw socket\n");
		return -1;
	}
	while(1)
	{
		n_read = recvfrom(sock_fd,buffer,2048,0,NULL,NULL);
		if(n_read<42)
		{
			printf("error when recv msg\n");
			return -1;
		}
		machead=(MAC_HEAD *)buffer;
		printf("MAC ADDRESS:%.2x:%02x:%02x:%02x:%02x:%02x ==> %.2x:%02x:%02x:%02x:%02x:%02x\n\n\n",machead->sourcemac[0],machead->sourcemac[1],machead->sourcemac[2],machead->sourcemac[3],machead->sourcemac[4],machead->sourcemac[5],machead->desmac[0],machead->desmac[1],machead->desmac[2],machead->desmac[3],machead->desmac[4],machead->desmac[5]);
		ipdata(buffer);
		switch(machead->type)
		{
			case 0x0008:printf("\n\nType : IP_DATA\n");ipdata(buffer);break;
			case 0x0608:printf("\n\nType : ARP_DATA\n");arp_data(buffer);break;
			case 0x3580:printf("\n\nType : RARP_DATA\n");arp_data(buffer);break;
			default:printf("unknow type\n");
		}
	}
	return 0;
}
