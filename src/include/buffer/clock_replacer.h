#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <cstdint>
#include <list>
#include <mutex>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"


class ClockReplacer : public Replacer{

public:
	explicit ClockReplacer(size_t num_pages);

	~ClockReplacer() override;

	bool Victim(frame_id_t *frame_id) override;

	void Pin(frame_id_t frame_id) override;

	void Unpin(frame_id_t frame_id) override;

	size_t Size() override;
private:
	const size_t num_pages_;
	size_t num_present_;
	bool * present_;
	bool * ref_;
	size_t clock_pointer_;
};


#endif