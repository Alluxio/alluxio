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
#include "struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <errno.h>
#include <arpa/inet.h>
#include <signal.h>
#include <netinet/in.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#define ICMP_ECHOREPLY 0 //echo应答
#define ICMP_ECHO 8 //echo请求

#define BUFSIZE 1500 //发送缓存最大值
#define DEFAULT_LEN 56 //ping消息默认数据大小
#define IP_HSIZE sizeof(IP_HEADER) //ip头部长度
#define IPVERSION 4 //ip version为ipv4
void alarm_handler(int);//SIGALRM处理程序
void int_handler(int);//SIGINT处理程序
char *hostname;//被ping的主机名
int datalen = DEFAULT_LEN;//ICMP消息携带的数据长度
char sendbuf[BUFSIZE];//发送数据包数组
char recvbuf[BUFSIZE];//接收数据包数组
int nsent;//发送的ICMP消息序号

int nrecv;//接收的ICMP消息信号
pid_t pid;//ping程序的进程PID
struct timeval recvtime;//接收ICMP应答的时间戳
int sockfd;//发送和接收原始嵌套字
struct sockaddr_in dest;//被ping的主机ip
struct sockaddr_in from;//发送ping应答消息的主机IP
struct sigaction act_alarm;
struct sigaction act_int;


//设置的时间是一个结构体，倒计时设置，重复倒时，超时值设为1秒，每隔1秒发送一次数据包
struct itimerval val_alarm = {
	.it_interval.tv_sec = 1, // 计时器重启动的间歇值：秒
	.it_interval.tv_usec = 0,//计时器重启动的间歇值：毫秒
	.it_value.tv_sec = 0,//计时器启动的初始值：秒
	.it_value.tv_usec = 1//计时器启动的初始值：毫秒
};


//错误报告
void bail(const char *on_what)
{
	fputs(strerror(errno),stderr);
	fputs(":",stderr);
	fputs(on_what,stderr);
	fputc('\n',stderr);

	exit(1);
}
//计算校验和
unsigned short checksum(unsigned char *buf,int len)
{
	unsigned int sum=0;
	unsigned short *cbuf;
	cbuf=(unsigned short *)buf;
	while(len>1)
	{
		sum+=*cbuf++;
		len-=2;
	}
	if(len)
		sum+=*(unsigned char *)cbuf;
	sum=(sum>>16)+(sum&0xffff);
	sum+=(sum>>16);
	return ~sum;
}

//ICMP应答消息处理
int handle_pkt()
{
	IP_HEADER *ip;
	ICMP_DATA *icmp;
	int ip_hlen;
	unsigned short ip_datalen;
	double rtt;
	struct timeval *sendtime;

	ip = (IP_HEADER *)recvbuf;
	ip_hlen = (ip->ver_hlen&0xf)<<2;
	ip_datalen=ntohs(ip->data_length)-ip_hlen;
	
	icmp = (ICMP_DATA *)(recvbuf + ip_hlen);
	if(checksum((unsigned char *)icmp,ip_datalen))
		return -1;
	if(icmp->icmp_id!=pid)
		return -1;

	sendtime = (struct timeval *)icmp->data;
	rtt=((&recvtime)->tv_sec - sendtime->tv_sec)*1000+((&recvtime)->tv_usec - sendtime->tv_usec)/1000.0;

	printf("%d bytes from %s:icmp_seq=%u ttl=%d rtt=%0.3f ms\n",ip_datalen,inet_ntoa(from.sin_addr),icmp->icmp_seq,ip->ttl,rtt);

	return 0;
}

//设置信号处理程序
void set_sighandler()
{
	act_alarm.sa_handler = alarm_handler;//每隔1秒发送一次数据包
	if(sigaction(SIGALRM,&act_alarm,NULL)==-1)
	{
		bail("SIGALRM handler setting fails.");
	}

	act_int.sa_handler = int_handler;//初始化
	if(sigaction(SIGINT,&act_int,NULL)==-1)
	{
		bail("SIGALRM handler setting fails.");
	}
}

//统计ping命令的检测结果
void get_statistics(int nsent,int nrecv)
{
	printf("--%s ping statistics ---\n",inet_ntoa(dest.sin_addr));
	printf("%d packets transmitted, %d received, %0.0f%%""packet loss\n",nsent,nrecv,1.0*(nsent-nrecv)/nsent*100);
}

//中断信号处理函数
void int_handler(int sig)
{
	get_statistics(nsent,nrecv);//统计ping命令的检测结果
	close(sockfd);//关闭网络嵌套字
	exit(1);//退出程序
}
//发送ping消息
void send_ping()
{
	IP_HEADER *ip_hdr;
	ICMP_DATA *icmp_hdr;
	int len;
	int len1;

	ip_hdr = (IP_HEADER *)sendbuf;
	ip_hdr->ver_hlen = (sizeof(IP_HEADER)>>2)+(IPVERSION<<4);
	ip_hdr->tos=0;
	ip_hdr->data_length=IP_HSIZE+ICMP_HSIZE+datalen;
	ip_hdr->ident=0;
	ip_hdr->flags = 0;
	ip_hdr->offset=0;
	ip_hdr->proto=IPPROTO_ICMP;
	ip_hdr->ttl=255;
	ip_hdr->destip[0]=dest.sin_addr.s_addr&0xff;
	ip_hdr->destip[1]=(dest.sin_addr.s_addr&0xff00)>>8;
	ip_hdr->destip[2]=(dest.sin_addr.s_addr&0xff0000)>>16;
	ip_hdr->destip[3]=(dest.sin_addr.s_addr&0xff000000)>>24;
	len1=sizeof(IP_HEADER);
	icmp_hdr=(ICMP_DATA *)(sendbuf+len1);
	icmp_hdr->type=8;
	icmp_hdr->code=0;
	icmp_hdr->icmp_id=pid;
	icmp_hdr->icmp_seq=nsent++;
	memset(icmp_hdr->data,0xff,datalen);//讲datalen中的datalen各自己替换为0xff并返回icmp_hdr->data

	gettimeofday((struct timeval *)icmp_hdr->data,NULL);//获取当前时间
	len=ip_hdr->data_length;
	icmp_hdr->checksum=0;
	icmp_hdr->checksum=checksum((unsigned char *)icmp_hdr,len);	
	sendto(sockfd,sendbuf,len,0,(struct sockaddr *)&dest,sizeof(dest));//经socket传送数据
}
void alarm_handler(int signo)
{
	send_ping();
}

//接收程序发送的ping命令的应答
void recv_reply()
{
	int n;
	int len;
	int errno;
	n=0;
	nrecv=0;
	len=sizeof(from);
	while(1)
	{
		if((n=recvfrom(sockfd,recvbuf,sizeof(recvbuf),0,(struct sockaddr *)&from,&len))<0)
		{
			if(errno==EINTR)
				continue;
			bail("recvfrom error");
		}
		gettimeofday(&recvtime,NULL);
		if(handle_pkt())
			continue;
		nrecv++;
	}
	get_statistics(nsent,nrecv);
}


int main(int argc,char *argv[])
{
	struct hostent *host;
	int on=1;
	if(argc!=2)
	{
		printf("unknow ip\n");
		return -1;
	}
	if((host=gethostbyname(argv[1]))==NULL)
	{
		printf("usage:%s hostname/IP address\n",argv[0]);
		exit(1);
	}
	hostname=argv[1];
	memset(&dest,0,sizeof(dest));
	dest.sin_family=PF_INET;
	dest.sin_port=ntohs(0);
	dest.sin_addr=*(struct in_addr *)host->h_addr_list[0];
	//printf("%X\n",dest.sin_addr.s_addr);
	if((sockfd=socket(PF_INET,SOCK_RAW,IPPROTO_ICMP))<0)
	{
		perror("RAW socket created error");
		exit(1);
	}
	setsockopt(sockfd,IPPROTO_IP,IP_HDRINCL,&on,sizeof(on));

	set_sighandler();
	printf("Ping %s(%s): %d bytes data in ICMP packets.\n",argv[1],inet_ntoa(dest.sin_addr),datalen);
	
	if((setitimer(ITIMER_REAL,&val_alarm,NULL))==-1)
	{
		bail("setitimer fails.");
	}
	recv_reply();
	return 0;
}
