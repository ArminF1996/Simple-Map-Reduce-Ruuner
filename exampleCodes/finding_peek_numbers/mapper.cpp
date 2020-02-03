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
	string op, number;
	int st,en;
	while(cin>>op>>number>>st>>en){
		for(int i=st ; i<=en ; i++){
			s[i].insert(make_pair(op, number));
		}
	}
	for(int i=0 ; i<86400 ; i++){
		set<string> a,b,c;
		a.clear();
		b.clear();
		c.clear();
		for(auto x : s[i]){
			if(x.first == "mci")
				a.insert(x.second);
			if(x.first == "mtn")
				b.insert(x.second);
			if(x.first == "rtl")
				c.insert(x.second);
		}
		cout<<"mci "<<i;
		for(auto x: a){
			cout<<' '<<x;
		}
		cout<<"\nmtn "<<i;
		for(auto x: b){
			cout<<' '<<x;
		}
		cout<<"\nrtl "<<i;
		for(auto x: c){
			cout<<' '<<x;
		}
		cout<<endl;
	}
	return 0;
}

