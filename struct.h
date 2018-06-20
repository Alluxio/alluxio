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
typedef struct iphdr
{
	unsigned char ver_hlen;//4位首部长度+4位IP版本号
	unsigned char tos;//服务类型
	unsigned short data_length;//ip包总长度
	unsigned short ident;//标识字段
	unsigned int flags:3;//标记字段
	unsigned int offset:13;//分段偏移量
	unsigned char ttl;//生存期
	unsigned char proto;//协议类型
	unsigned short checksum;//ip首部校验和
	unsigned char sourceip[4];//源ip地址
	unsigned char destip[4];//目的ip地址
}IP_HEADER;

typedef struct machead
{
	unsigned char desmac[6];//目的mac地址
	unsigned char sourcemac[6];//源mac地址
	unsigned int type:16;//数据包类型号
	IP_HEADER iphdr;
}MAC_HEAD;

typedef struct arpqr
{
	unsigned short ar_hrd;//硬件类型
	unsigned short ar_pro;//协议类型
	unsigned char ar_hln;//硬件地址长度
	unsigned char ar_pln;//协议地址长度
	unsigned short optype;//操作类型
	unsigned char arp_sha[6];//发送者硬件地址
	unsigned char arp_spa[4];//发送者ip地址
	unsigned char arp_tha[6];//目标硬件地址
	unsigned char arp_tpa[4];//目标ip地址
}ARP_RARP;


typedef struct icmpdata
{
	unsigned char type;//消息类型
	unsigned char code;//消息代码
	unsigned short checksum;//校验和
	union
	{
		struct
		{
			unsigned short id;
			unsigned short sequence;
		}echo;
		unsigned int gateway;
		struct
		{
			unsigned short unsed;
			unsigned short mtu;
		}frag;
	}un;
	unsigned char data[0];//ICMP数据占位符
	#define icmp_id un.echo.id
	#define icmp_seq un.echo.sequence
}ICMP_DATA;
#define ICMP_HSIZE sizeof(ICMP_DATA)

typedef struct udphdr
{
	unsigned short sport;//源端口
	unsigned short dport;//目的端口
	unsigned int len;//UDP包长度
	unsigned int sum;//校验和
}UDP_HEADER;

typedef struct tcphdr
{
	unsigned short sport;//源端口
	unsigned short dport;//目的端口
	unsigned int seq;//序列号
	unsigned int ack;//确认号
	unsigned char lenres;//首部长度
	unsigned short flag:6;//标志位
	unsigned short win;//流量控制窗口大小
	unsigned short sum;//校验和
	unsigned short urp;//紧急数据便宜量
}TCP_HEADER;

