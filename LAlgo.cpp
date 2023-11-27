// #include <iostream>
#include <bits/stdc++.h>
#include <mutex>
#include <unistd.h>
#include <pthread.h>
#include<sys/time.h>
#include<fstream>
#include<string>
#include <iostream>

using namespace std;

double l;
int numVar,numLocks;
ofstream output;
default_random_engine generator;
exponential_distribution<double> distribution(l);
// pthread_mutex_t* ownership[100];
pthread_mutex_t counter_lock, file_lock;
int counter_id = 0;
float time_com = 0.0;
float tot_aborts = 0.0;

vector<pthread_mutex_t*> ownership;

struct dataItem{
    int id;                 // id of the data item
    int val;                // value of the data item
    vector<int> rList;      // list of transactions that have read the data item
    vector<int> wList;      // list of transactions that have written the data item
};

// Class for transaction object
class transaction{
    public:
        int id;                 // id of the transaction
        int status; 
        int aborts;            // 0-> live, 1-> commit, 2-> abort
        set<int> rSet;       // set of data items read by the transaction
        map<int, int> wSet;   // set of data items written by the transaction
        vector<int> L;
        struct timeval start, end;
        transaction(int id){
            this->id = id;
            this->status = 0;
            this->aborts = 0;
            gettimeofday(&this->start, NULL);
        }
};

map<int, dataItem*> shared;        // vector of data items
map<int, transaction*> tx;      // map of transaction objects


string convertTime(time_t epoch_time){
	tm *t = localtime(&epoch_time);
	string ct = to_string(t->tm_hour)+":"+to_string(t->tm_min)+":"+to_string(t->tm_sec);
	return ct;
}

transaction* begin_trans() {      //begin transaction and returns an unique id
    pthread_mutex_lock(&counter_lock);
    counter_id++;
    int id_val = counter_id;
    pthread_mutex_unlock(&counter_lock);

    transaction* new_t = new transaction(id_val);
    tx.insert(pair<int, transaction*>(id_val, new_t));
	return new_t;
}


int h(int x) {
    return x%numLocks;
}


void Rollback(map<int,int> writeSet) {
	for(auto itr = writeSet.begin(); itr != writeSet.end(); ++itr) {
		if(itr->second != -1) {
            dataItem* tp = shared[itr->first];
			tp->val = itr->second;
		}
	}
}

void* updtMem(void* arg) {
	int m = 100;
	struct timeval critStartTime, critEndTime;

	gettimeofday(&critStartTime,NULL);

	transaction *t = begin_trans();
	map<int,int> accessedLocations;
    vector<int> data_items;
	map<int,int> writeSet;
	int iters = rand()%20;	
	int aborts = 0;
	for(int i = 0; i < m; i++) {
		accessedLocations[i] = 0;
        data_items.push_back(i);
	}
	for(int i = 0; i < iters ;i++) {
		int r = rand()%(data_items.size());
        int randInd = data_items[r];
        data_items.erase(data_items.begin()+r);

        int hx = h(randInd);
        vector<int> M;
        vector<int>::iterator k = find(t->L.begin(), t->L.end(), hx);
        if(k==t->L.end()) {
            for(auto it=t->L.begin();it!=t->L.end();it++) {
                if(*it>hx) {
                    M.push_back(*it);
                }
            }
            sort(M.begin(), M.end());
            t->L.push_back(hx);
            if(M.size()==0) {
                pthread_mutex_lock(ownership[hx]);
            }
            else if(pthread_mutex_trylock(ownership[hx])!=0) {
                Rollback(writeSet);
                for(int i=0;i<M.size();i++) {
                    pthread_mutex_unlock(ownership[M[i]]);
                }

                pthread_mutex_lock(ownership[hx]);
                for(int j=0;j<M.size();j++) {
                    pthread_mutex_lock(ownership[M[j]]);
                }
                pthread_mutex_lock(&file_lock);
                output <<"Transaction "<<t->id<< " aborted and restarted.\n";
                aborts++;
                pthread_mutex_unlock(&file_lock);
            }
        }

        dataItem *data = shared[randInd];
        struct timeval readTime, writeTime;

		gettimeofday(&readTime,NULL);
        pthread_mutex_lock(&file_lock);
		output<<"Thread id "<<pthread_self()<<" Transaction "<<t->id<<" reads from  "<< randInd<<" value "<<data->val<<" at time "<<convertTime(readTime.tv_sec)<<"\n";
		pthread_mutex_unlock(&file_lock);

        writeSet[r] = data->val;
		data->val += rand()%10;
        gettimeofday(&writeTime, NULL);
        pthread_mutex_lock(&file_lock);
		output<<"Thread id "<<pthread_self()<<" Transaction "<<t->id<<" writes to "<< randInd<<" value "<<data->val<<" at time "<<convertTime(writeTime.tv_sec)<<"\n";
        pthread_mutex_unlock(&file_lock);


		float randTime = distribution(generator);
        usleep(randTime*1000000);
	}


    // Commit the transaction and release all locks in L.
    for(int i=0;i<t->L.size();i++) {
		pthread_mutex_unlock(ownership[t->L[i]]);
	}

	gettimeofday(&critEndTime,NULL);
	pthread_mutex_lock(&file_lock);
    tot_aborts += aborts;
    time_com += critEndTime.tv_sec - critStartTime.tv_sec + critEndTime.tv_usec/1000000.0 - critStartTime.tv_usec/1000000.0;
	output << "Commit time value : " << critEndTime.tv_sec - critStartTime.tv_sec + critEndTime.tv_usec/1000000.0 - critStartTime.tv_usec/1000000.0 << " for thread " << pthread_self() << endl;
	pthread_mutex_unlock(&file_lock);
}

int main() {
	int numTrans;
	ifstream input;
	input.open("inp-params1.txt");
	output.open("cm-log.txt");

    input>> numTrans >> numVar >> numLocks >> l;


    pthread_mutex_init(&counter_lock, NULL);
    pthread_mutex_init(&file_lock, NULL);

    for (int i=0; i<numVar; i++){
        dataItem* temp = new dataItem;
        temp->id = i;
        temp->val = 0;

        shared[i] = temp;
    }

    for (int i=0; i<numLocks; i++){
        pthread_mutex_t* tp = new pthread_mutex_t;
        pthread_mutex_init(tp, NULL);
        ownership.push_back(tp);
    }

	pthread_t threads[numTrans];
	for(int i = 0 ; i < numTrans; i++) {
		pthread_create(&threads[i],NULL,updtMem, NULL);
	} 
	for(int i = 0 ; i < numTrans ; i++){
		pthread_join(threads[i],NULL);
	}
    cout << "Average time taken for each transaction to commit is " << time_com/(float)numTrans << " seconds." << endl;
    cout << "Average number of aborts are: " << tot_aborts/(float)numTrans << endl;
}