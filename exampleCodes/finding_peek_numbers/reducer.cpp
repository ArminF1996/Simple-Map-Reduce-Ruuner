/* In The Name Of God */
#include <bits/stdc++.h>

# define xx first
# define yy second
# define pb push_back
# define pp pop_back
# define eps 1e-9
# define err(x) cerr << #x << " ::   " << x << endl;

using namespace std;
typedef long long ll;
typedef pair<int,int> pii;
typedef vector<int> vint;

set<pair<string,string> > s[86400];

int mci,rtl,mtn;

int main(int argc, char **argv){
	ios_base::sync_with_stdio (0);
	freopen(argv[1], "r", stdin);
	freopen(argv[2], "w", stdout);
	string line;
	while(getline(cin, line)){
		stringstream myStream(line);
		string op,number;
		int t;
		myStream >> op >> t;
		while(myStream >> number){
			s[t].insert(make_pair(op, number));
		}
	}
	for(int i=0 ; i<86400 ; i++){
		int tmp_mci = 0;
		int tmp_mtn = 0;
		int tmp_rtl = 0;
		for(auto x : s[i]){
			if(x.first == "mci")
				tmp_mci++;
			if(x.first == "mtn")
				tmp_mtn++;
			if(x.first == "rtl")
				tmp_rtl++;
		}
		mci = max(mci, tmp_mci);
		rtl = max(rtl, tmp_rtl);
		mtn = max(mtn, tmp_mtn);
	}
	cout<<"mci: "<<mci<<"\nmtn: "<<mtn<<"\nrtl: "<<rtl<<endl;
	return 0;
}

