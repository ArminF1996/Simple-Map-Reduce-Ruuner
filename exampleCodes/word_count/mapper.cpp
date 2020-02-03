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

map<string, int> mp;

int main(int argc, char **argv){
	ios_base::sync_with_stdio (0);
	freopen(argv[1], "r", stdin);
	freopen(argv[2], "w", stdout);
	string str;
	while(cin>>str){
		mp[str]++;
	}
	for(auto x: mp){
		cout<<x.first<<' '<<x.second<<endl;
	}
	return 0;
}

