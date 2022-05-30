#include <iostream>
using namespace std;
class T{public:int x;};
int main(){
	char * c = new char[sizeof(T)];
	c[0] = 10;
	c[1] = c[2] = c[3] = 0;
	T * t = reinterpret_cast<T*>(c);
	cout << t->x << endl;
	cout << (reinterpret_cast<T*>(c))->x << endl;
	return 0;
}
