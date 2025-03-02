#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dirent.h>
#include <unistd.h>
#include <time.h>

#define DATA_DIR "/workspace/data"
#define DATA_OUT "/workspace/data-out.csv"
#define MISSING_VALUE 32766
#define tdf 50

struct OriginalData {short originaldata[11];};
//0编号、1纬度、2经度、3年、4月、5日、6平均、7最大、8最小、9到最后一天应该有的记录数、10行号从1开始
struct FirstReduce {short firstreduce[10];};
//0编号、1纬度、2经度、3年、4月、5日、6平均、7最大、8最小、9数据的tag
struct Reduce {short reduce[9];};
//0编号、1纬度、2经度、3年、4月、5日、6平均、7最大、8最小
struct Process {short process[9];};
//0编号、1纬度、2经度、3年，4年平均、5年最大、6年最小、7年升温、8年降温

short calrise(short dave[], short i) {
    short rise = 0;
    short j = 0;
    while(j < i){
        if (dave[j+1]-dave[j] >= tdf) {rise++; j++;}
        else if (dave[j+2]-dave[j] >= tdf) {if(j+2>i) break; rise++; j+=2;}
        else if (dave[j+3]-dave[j] >= tdf) {if(j+3>i) break; rise++; j+=3;}
        else j++;
    }
    return rise;
}

short caldrop(short dave[], short i) {
    short drop = 0;
    short j = 0;    
    while(j < i){
        if (dave[j+1]-dave[j] <= -tdf) {drop++; j++;}
        else if (dave[j+2]-dave[j] <= -tdf) {if(j+2>i) break; drop++; j+=2;}
        else if (dave[j+3]-dave[j] <= -tdf) {if(j+3>i) break; drop++; j+=3;}
        else j++;
    }
    return drop;
}


//导入数据，基础转换
size_t import_data(struct OriginalData **od, FILE *fp, size_t i) {
    char line[255];    //一行原始数据
    short lines = 0;    //一个站点中的第几行
    while (fgets(line, sizeof(line), fp)){
        int number;    //记录处理前编号
        short wd, jd;    //处理前的纬度和经度

        //为结构体分配内存，并检查内存分配是否成功
        struct OriginalData *od_temp = realloc(*od, (i+1) * sizeof(struct OriginalData));
        if (od_temp == NULL) {free(*od); *od=NULL; return -1;}
        *od = od_temp;

        //读取最原始的数据
        sscanf(line, "%d %hd %hd %*d %hd %hd %hd %hd %hd %hd",
        &number, &wd, &jd, &(*od)[i].originaldata[3], &(*od)[i].originaldata[4],
        &(*od)[i].originaldata[5], &(*od)[i].originaldata[6],
        &(*od)[i].originaldata[7], &(*od)[i].originaldata[8]);
        lines++;    //行数从1开始

        //转换纬度
        short m, n;
        m = wd / 100;
        m = m * 100;
        n = (wd - m) * 100 / 60;
        (*od)[i].originaldata[1] = (short) m + n;
        //转换经度
        m = jd / 100;
        m = m * 100;
        n = (jd - m) * 100 / 60;
        (*od)[i].originaldata[2] = (short) m + n;

        (*od)[i].originaldata[0] = (short) number - 50000;    //编号
        (*od)[i].originaldata[10] = lines;    //行号从1开始

        //计算到最后一天应该有的记录数
        struct tm startDate = {0}; 
        startDate.tm_year = (*od)[i].originaldata[3] - 1900; 
        startDate.tm_mon = (*od)[i].originaldata[4] - 1;
        startDate.tm_mday = (*od)[i].originaldata[5] - 1;
        time_t start = mktime(&startDate);
        size_t df = 1609344000-start;
        (*od)[i].originaldata[9]= df / (60 * 60 * 24);

        i++;    //开始写下一个结构体
    }

    fclose(fp);
    printf("成功导入%zu条数据\n", i);
    return i;    //返回导入数据的条数
}

//去除连续无记录及其之前的数据，标记可用值
size_t first_reduce(struct OriginalData *od, struct FirstReduce **fr, size_t i, short tag[]){
    size_t k=0;
    //去除连续无记录及其之前的数据
    for(size_t j=0; j<i; j++){
        if (i - od[j].originaldata[10] == od[j].originaldata[9]){
            //分配内存并检验
            struct FirstReduce *fr_temp = realloc(*fr, (k+1) * sizeof(struct FirstReduce));
            if (fr_temp == NULL) {free(*fr); return -1;}
            *fr = fr_temp;
            (*fr)[k].firstreduce[0] = od[j].originaldata[0];
            (*fr)[k].firstreduce[1] = od[j].originaldata[1];
            (*fr)[k].firstreduce[2] = od[j].originaldata[2];
            (*fr)[k].firstreduce[3] = od[j].originaldata[3];
            (*fr)[k].firstreduce[4] = od[j].originaldata[4];
            (*fr)[k].firstreduce[5] = od[j].originaldata[5];
            (*fr)[k].firstreduce[6] = od[j].originaldata[6];
            (*fr)[k].firstreduce[7] = od[j].originaldata[7];
            (*fr)[k].firstreduce[8] = od[j].originaldata[8];
            k++;
        }
    }

    //标记可用值
    short count = 0;
    short flag = 0;    //暂时储存tag值
    for(size_t j=0; j<k; j++){
        if ((*fr)[j].firstreduce[6] == 32766 || (*fr)[j].firstreduce[7] == 32766 || (*fr)[j].firstreduce[8] == 32766) {
            count++;
        }else if(count > 3){
            tag[(*fr)[j].firstreduce[0]]++;
            flag++;
            count = 0;
        }else{
            count = 0;
        }
        (*fr)[j].firstreduce[9] = flag;
    }

    free(od);
    printf("完成第一次删减，去除连续无记录及其之前的数据，剩余%zu条数据\n", k);
    return k;
}

//去除连续空缺值和开头空缺值, 获取起始年份
size_t second_reduce(struct FirstReduce *fr, size_t i, short tag[], struct Reduce **re, short *startyear){
    size_t k=0;
    char flag = 0;    //从开头开始的没有空缺
    for (size_t j=0; j<i; j++){
        //去除开头空缺值
        if(fr[j].firstreduce[6] != 32766 && fr[j].firstreduce[7] != 32766 && fr[j].firstreduce[8] != 32766){
            flag = 1;
        }

        //去除连续空缺值
        if (fr[j].firstreduce[9] == tag[fr[j].firstreduce[0]] && flag) {
            //分配内存并检验
            struct Reduce *re_temp = realloc(*re, (k+1) * sizeof(struct Reduce));
            if (re_temp == NULL) {free(*re); return -1;}
            *re = re_temp;
            (*re)[k].reduce[0] = fr[j].firstreduce[0];
            (*re)[k].reduce[1] = fr[j].firstreduce[1];
            (*re)[k].reduce[2] = fr[j].firstreduce[2];
            (*re)[k].reduce[3] = fr[j].firstreduce[3];
            (*re)[k].reduce[4] = fr[j].firstreduce[4];
            (*re)[k].reduce[5] = fr[j].firstreduce[5];
            (*re)[k].reduce[6] = fr[j].firstreduce[6];
            (*re)[k].reduce[7] = fr[j].firstreduce[7];
            (*re)[k].reduce[8] = fr[j].firstreduce[8];
            k++;    //下一条数据
        }

    }

    //获取起始年份
    if((*re)[0].reduce[4]==1 && (*re)[0].reduce[5]==1){
        *startyear = (*re)[0].reduce[3];
    }else *startyear = (*re)[0].reduce[3] + 1;

    free(fr);
    printf("完成第二次删减，去除连续空缺值和开头空缺值，并获取起始年份%hd, 剩余%zu条数据\n", *startyear, k);
    return k;
}

//三次样条插值
void fillMissingValuesSpline(short data[], size_t length) {
    // 统计有效数据点数量
    int validCount = 0;
    for (int i = 0; i < length; i++) {
        if (data[i] != MISSING_VALUE) {
            validCount++;
        }
    }
    if (validCount < 2) return; // 有效数据不足以插值

    // 分配有效数据点的数组
    double *x = malloc(validCount * sizeof(double));
    double *y = malloc(validCount * sizeof(double));
    int idx = 0;
    for (int i = 0; i < length; i++) {
        if (data[i] != MISSING_VALUE) {
            x[idx] = i;
            y[idx] = data[i];
            idx++;
        }
    }

    // 计算步长和二阶导数
    int n = validCount - 1;
    double *h = malloc(n * sizeof(double));
    double *alpha = malloc(n * sizeof(double));
    double *l = malloc((n + 1) * sizeof(double));
    double *mu = malloc(n * sizeof(double));
    double *z = malloc((n + 1) * sizeof(double));

    for (int i = 0; i < n; i++) {
        h[i] = x[i + 1] - x[i];
        alpha[i] = (3 / h[i]) * (y[i + 1] - y[i]) - (3 / h[i - 1]) * (y[i] - y[i - 1]);
    }

    l[0] = 1; mu[0] = 0; z[0] = 0;
    for (int i = 1; i < n; i++) {
        l[i] = 2 * (x[i + 1] - x[i - 1]) - h[i - 1] * mu[i - 1];
        mu[i] = h[i] / l[i];
        z[i] = (alpha[i] - h[i - 1] * z[i - 1]) / l[i];
    }
    l[n] = 1; z[n] = 0;

    // 计算样条系数
    double *c = malloc((n + 1) * sizeof(double));
    double *b = malloc(n * sizeof(double));
    double *d = malloc(n * sizeof(double));
    c[n] = 0;
    for (int j = n - 1; j >= 0; j--) {
        c[j] = z[j] - mu[j] * c[j + 1];
        b[j] = (y[j + 1] - y[j]) / h[j] - h[j] * (c[j + 1] + 2 * c[j]) / 3;
        d[j] = (c[j + 1] - c[j]) / (3 * h[j]);
    }

    // 填补缺失值
    for (int i = 0; i < length; i++) {
        if (data[i] == MISSING_VALUE) {
            // 确定区间
            int j;
            for (j = 0; j < n; j++) {
                if (x[j] <= i && i <= x[j + 1]) break;
            }
            double dx = i - x[j];
            data[i] = (short)(y[j] + b[j] * dx + c[j] * dx * dx + d[j] * dx * dx * dx);
        }
    }

    // 释放内存
    free(x);
    free(y);
    free(h);
    free(alpha);
    free(l);
    free(mu);
    free(z);
    free(c);
    free(b);
    free(d);
}

//去除1970年1月1日之前的数据
size_t shave_head(struct Reduce *re, struct Reduce **red, size_t i, short startyear){
    size_t k=0;
    for (size_t j=0; j<i; j++){
        if (startyear <= 1970  && re[j].reduce[3] >= 1970){
            //分配内存并检验
            struct Reduce *red_temp = realloc(*red, (k+1) * sizeof(struct Reduce));
            if (red_temp == NULL) {free(*red); return -1;}
            *red = red_temp;

            (*red)[k].reduce[0] = re[j].reduce[0];
            (*red)[k].reduce[1] = re[j].reduce[1];
            (*red)[k].reduce[2] = re[j].reduce[2];
            (*red)[k].reduce[3] = re[j].reduce[3];
            (*red)[k].reduce[4] = re[j].reduce[4];
            (*red)[k].reduce[5] = re[j].reduce[5];
            (*red)[k].reduce[6] = re[j].reduce[6];
            (*red)[k].reduce[7] = re[j].reduce[7];
            (*red)[k].reduce[8] = re[j].reduce[8];
            k++;
        }
    }

    free(re);
    printf("成功去除1970年1月1日之前的数据, 剩余%zu条数据\n", k);
    return k;
}

//计算年值
size_t cal_yearly(struct Reduce *red, struct Process **pd, size_t i){
    size_t k = 0;

    size_t sum;
    short days = 0;
    short dave[366];
    short ymax;
    short ymin;
    for(size_t j=0; j<=i; j++){

        //保证最后一年能够被计算
        int year = red[j].reduce[3];
        if(j==i) year = 32766;

        if (year == red[j-1].reduce[3]){
            start:
            dave[days] = red[j].reduce[6];
            if(red[j].reduce[7] > ymax) ymax = red[j].reduce[7];
            if(red[j].reduce[8] < ymin) ymin = red[j].reduce[8];
            sum += red[j].reduce[6];
            days++;
        }else{
            if(days == 0){goto start;}
            //分配内存并检验
            struct Process *pd_temp = realloc(*pd, (k+1) * sizeof(struct Process));
            if (pd_temp == NULL) {free(*pd); return -1;}
            *pd = pd_temp;

            (*pd)[k].process[0] = red[j-1].reduce[0];
            (*pd)[k].process[1] = red[j-1].reduce[1];
            (*pd)[k].process[2] = red[j-1].reduce[2];
            (*pd)[k].process[3] = red[j-1].reduce[3];
            (*pd)[k].process[4] = sum / days;
            (*pd)[k].process[5] = ymax;
            (*pd)[k].process[6] = ymin;
            (*pd)[k].process[7] = calrise(dave, days);
            (*pd)[k].process[8] = caldrop(dave, days);
            k++;
            ymax = -1000;
            ymin = 1000;
            days = 0;
            sum = 0;
        }
    }

    free(red);
    printf("完成年值计算，有%zu年数据\n", k);
    return k;
}

int main(){
    int k=0;
    //打开目录
    chdir(DATA_DIR);
    DIR *dir = opendir(DATA_DIR);
    if (dir==NULL) {perror("failed to open DATA_DIR"); return 1;}
    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (strstr(ent->d_name, "tem.txt")){

            //打开文件
            FILE *fp = fopen(ent->d_name, "r");
            if(fp==NULL){
                printf("Failed to open %s\n", ent->d_name);
                return 1;
            }else{
                printf("\n正在处理%s\n", ent->d_name);
            }

            size_t i = 0;    //剩余的数据条数

            //导入数据，基础转换
            struct OriginalData *od = NULL;
            i = import_data(&od, fp, i);
            if (i==-1) {perror("Memory allocation failed at od"); free(od); return -1;}
            if (i==1) return 1;
            

            //去除连续无记录及其之前的数据, 标记可用值
            struct FirstReduce *fr = NULL;
            short tag[10000] = {0};    //数据可用的tag
            i = first_reduce(od, &fr, i, tag);
            if (i==-1) {perror("Memory allocation failed at fr"); free(fr); return -1;}

            //去除连续空缺值和开头空缺值, 获取起始年份
            struct Reduce *re = NULL;
            short startyear;
            i = second_reduce(fr, i, tag, &re, &startyear);
            if (i==-1) {perror("Memory allocation failed at re"); free(re); return -1;}

            //三次样条插值
            for(int p=6; p<=8; p++){
                short data[i];
                for(size_t j=0; j<i; j++){
                    data[j] = re[j].reduce[p];
                }
                fillMissingValuesSpline(data, i);
                for(size_t j=0; j<i; j++){
                    re[j].reduce[p] = data[j];
                }
            }
            printf("完成三次样条插值\n");

            //去除1970年1月1日之前的数据
            struct Reduce *red = NULL;
            i = shave_head(re ,&red, i, startyear);
            if (i==-1) {perror("Memory allocation failed at red"); free(red); return -1;}

            //计算年值
            if(i!=0){
                struct Process *pd = NULL;
                i = cal_yearly(red, &pd, i);
                if (i==-1) {perror("Memory allocation failed at pd"); free(pd); return -1;}

                //写出数据
                FILE *fp1 = fopen(DATA_OUT, "a");
                for(size_t j=0; j<i; j++){
                    for(int p=0; p<9; p++){
                        fprintf(fp, "%d", pd[j].process[p]);
                        if (p < 8) {
                            fprintf(fp1, ",");
                        }else{
                            fprintf(fp1, "\n");
                        }
                    }
                }
                fclose(fp1);

            }else{
                printf("数据不足，排除站点\n");
                continue;
            }
            
            k++;
        }
    }

    closedir(dir);
    printf("\n成功处理%d个文件\n", k);
    return 0;
}



