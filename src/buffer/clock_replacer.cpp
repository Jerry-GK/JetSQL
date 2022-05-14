#include "buffer/clock_replacer.h"
#include <cstddef>
#include <ctime>

ClockReplacer::ClockReplacer(size_t size)
	:num_pages_(size),
	num_present_(0),
	present_(new char[size]),
	ref_(new char[size]),
	clock_pointer_(0)
	{
		
}

ClockReplacer::~ClockReplacer(){
    // do nothing...
}

bool ClockReplacer::Victim(frame_id_t *frame_id){
	if(!num_present_)return false;
	while(num_present_){
		if(present_[clock_pointer_]){
			if(ref_[clock_pointer_])ref_[clock_pointer_] = 0;
			else{
				present_[clock_pointer_] = 0;
				ref_[clock_pointer_] = 0;
				num_present_--;
				*frame_id = clock_pointer_;
				return true;
			}
		}
		clock_pointer_ = (clock_pointer_ + 1) % num_pages_;
	}
	return false;
}

void ClockReplacer::Pin(frame_id_t frame_id){
	if((unsigned)frame_id > num_pages_ || num_present_ == 0) return;
	if(!present_[frame_id])return ;
	present_[frame_id] = 0;
	ref_[frame_id] = 0;
	num_present_ --;
}

void ClockReplacer::Unpin(frame_id_t frame_id){
	if((unsigned)frame_id > num_pages_)return ;
	if(present_[frame_id])return;
	present_[frame_id] = 1;
	ref_[frame_id] = 1;
	num_present_ ++;
}

size_t ClockReplacer::Size(){
	return num_present_;
}
