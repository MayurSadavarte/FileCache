#include"file_cache.h"
#include<stdlib.h>
#include<stdio.h>
#include<map>
#include<list>
#include<pthread.h>
#include<fstream>
#include<assert.h>

/*
 * NOTE:
 * Worst-case Complexities: ( n here stands for max_cache_entries_ )
 * PinFiles: O(1)
 * UnpinFiles: O(1)
 * FileData: O(1)
 * MutableFileData: O(1)
 * Assumptions:
 * 1. Pinning the file before reading/writing/unpinning is a
 *	  responsibility of individual threads. Otherwise we will have
 *	  to keep track of thread_ids which have pinned the file. Only
 *	  such threads would be allowed to do further operations !
 * 2. The existing files will also be of the size of minimum 10240000 
 * 	  bytes. (10KB)
 * Possible Improvements:
 * 1. Current locking scheme has only single lock per data structure.
 *	  The same lock serves for both readers and writers. Instead
 * 	  more efficient locking scheme like Readers-Writer lock can be
 *	  used.
 * 2. In current implementation, dirty file cache buffers are written
 * 	  synchronously to the disk. The eviction algorithm waits for the
 *	  write operation to complete to pin the buffer. Instead, this can
 * 	  be done in asynchronous manner where eviction thread just 
 *	  initiates an asynchronous operation and picks the next free
 * 	  buffer from the free list and some other thread or POSIX
 * 	  AIO thread callback can put this buffer back into the free buffers
 * 	  list whenever async IO completes.
 */
	

// File cache buffer status -
// pinned/unpinned - 1st bit
// clean/dirty - 2nd bit
#define PINNED 1
#define DIRTY 2
#define FILESIZE 10240000

class FileCacheImpl : public FileCache {
	private:
		// This class constitues an individual element in the file cache.
		// There is one to one mapping between file and WriteCacheBuffer.
		class WriteCacheBuffer {
			public:
				std::string fileName;
				// Multiple threads can pin the file
				unsigned int pinCount;
				// File cache buffer status -
				// pinned/unpinned - 1st bit
				// clean/dirty - 2nd bit
				unsigned int status;	

				WriteCacheBuffer *next;
				WriteCacheBuffer *prev;
				
				// Stores actual data of the file	
				char buffer[ FILESIZE ];			
	
				WriteCacheBuffer( std::string f ) :
					fileName( f ),
					pinCount( 0 ),
					status( 0 ),
					next( NULL ),
					prev( NULL ) {
					std::fill( buffer, buffer + FILESIZE, 0 );
					if ( f.compare( "dummy" ) != 0 ) {
						initialize();
					}
				}

				void initialize() {
					std::ifstream rstream;
					rstream.open( fileName.c_str(), 
								  std::ios::binary | std::ios::in );
					if ( !rstream.is_open() ) {
						// File does not exist
						printf( "File %s does not exist. Hence creating one.\n", 
								fileName.c_str() );
						std::ofstream wstream;
						wstream.open( fileName.c_str(), 
									  std::ios::binary | std::ios::out );
						assert( wstream.is_open() );
						status = status | DIRTY;
						wstream.close();
					} else {
						rstream.seekg( 0, std::ios::beg );
						rstream.read( buffer, FILESIZE );
						rstream.close();
					}
				} 

				// This function is called by the eviction algorithm
				// when the existing buffer is reused for different file
				void reinitialize( std::string f ) {
					fileName = f;	
					pinCount = 0;
					status = 0;
					std::fill( buffer, buffer + FILESIZE, 0 );
					initialize();	
				}
				
				// Dirty buffer must be written back to disk before
				// reusage	
				void writeBack() {
					std::ofstream wstream;
					wstream.open( fileName.c_str(), 
								  std::ios::binary | std::ios::out );
					assert( wstream.is_open() );	
					wstream.seekp( 0, std::ios::beg );
					wstream.write( buffer, FILESIZE );
					wstream.close();
					status = status & ~DIRTY;	
				}	
			private:
				// Disable default constructor
				WriteCacheBuffer() {}
		};	
		
		// Safe access for file cache map, free buffers list
		pthread_mutex_t mapMut_;
		pthread_mutex_t freeListMut_;
		pthread_cond_t freeListCV_;
	
		// File cache map stores -
		// fileName <--> WriteCacheBuffer	
		std::map<std::string, WriteCacheBuffer *> writeCache_;	
		// Keeps track of unpinned buffer.
		// Unpinned buffer might be Dirty. Dirty buffers are written
		// back to the disk before reusage.
		WriteCacheBuffer *freeListHead_;
		unsigned int freeListSize_;

		void freeListInit() {
			std::string dummystr( "dummy" );
			freeListHead_ = new WriteCacheBuffer( dummystr );
			freeListHead_->next = freeListHead_;
			freeListHead_->prev = freeListHead_;
			freeListSize_ = 0;
		}
		
		// The caller should lock the free list appropriately before calling this function
		void freeList_push_back( WriteCacheBuffer *buf ) {
			assert( buf );
			WriteCacheBuffer *prevToHead = freeListHead_->prev;
			buf->prev = prevToHead;
			buf->next = freeListHead_;
			prevToHead->next = buf;
			freeListHead_->prev = buf;
			freeListSize_++;
		}

		// The caller should lock the free list appropriately before calling this function
		WriteCacheBuffer *freeList_pop_front() {
			WriteCacheBuffer *retBuf = freeListHead_->next;
			assert( retBuf );
			WriteCacheBuffer *nextToNext = retBuf->next;
			assert( nextToNext );
			retBuf->next = NULL;
			retBuf->prev = NULL;
			freeListHead_->next = nextToNext;
			nextToNext->prev = freeListHead_;
			freeListSize_--;
			return retBuf;
		}

		// The caller should lock the free list appropriately before calling this function
		void freeList_pop( WriteCacheBuffer *buf ) {
			assert( buf );
			WriteCacheBuffer *next = buf->next;
			WriteCacheBuffer *prev = buf->prev;
			assert( next );
			assert( prev );
			prev->next = next;
			next->prev = prev;
			buf->next = NULL;
			buf->prev = NULL;
			freeListSize_--;
		}
	public:
		FileCacheImpl( int max_cache_entries ) :
			FileCache( max_cache_entries ) {
			pthread_mutex_init( &mapMut_, NULL );
			pthread_mutex_init( &freeListMut_, NULL );
			freeListInit();
		}

		~FileCacheImpl() {
			// File cache map destroy
			// Free buffers list head destroy
			// Mutex, Condition variable destroy
			pthread_mutex_lock( &mapMut_ );
			std::map<std::string, WriteCacheBuffer *>::iterator mapIter;

			mapIter = writeCache_.begin();
			while ( mapIter != writeCache_.end() ) {
				WriteCacheBuffer *buffer = mapIter->second;
				if ( buffer->status & DIRTY ) {

					buffer->writeBack();
				}
				mapIter++;
			}
			writeCache_.clear();
			pthread_mutex_unlock( &mapMut_ );

			pthread_mutex_lock( &freeListMut_ );
			delete freeListHead_;
			pthread_mutex_unlock( &freeListMut_ );
			pthread_mutex_destroy( &mapMut_ );
			pthread_mutex_destroy( &freeListMut_ );
			pthread_cond_destroy( &freeListCV_ );
		}

		virtual void PinFiles( const std::vector<std::string>& file_vec );			
  		virtual void UnpinFiles( const std::vector<std::string>& file_vec );
  		
		virtual const char *FileData( const std::string& file_name );
  
		virtual char *MutableFileData( const std::string& file_name );
};



void
FileCacheImpl::PinFiles( const std::vector<std::string>& file_vec ) {
	// Lock the file cache map
	// Iterate through file_vec
	// Check whether file buffer already exists in file cache map
	// If exists, pin the buffer
	// If buffer does not exist, check whether the limit of 
	//	max number of entries in cache is met
	// If not, read a file and create new buffer, put it in file cache map
	// If limit met, run the eviction algorithm
	// Lock the list
	// Get the next buffer from free list, modify the buffer and put it on file cache map
	// Unlock list
	// Unlock file cache map

	pthread_mutex_lock( &mapMut_ );
	std::vector<std::string>::const_iterator fileIter;
	for ( fileIter = file_vec.begin();
		  fileIter != file_vec.end();
		  ++fileIter ) {
		std::map<std::string, WriteCacheBuffer *>::iterator mapIter;

		if ( (mapIter = writeCache_.find( *fileIter )) != writeCache_.end() ) {
			// Buffer found in file cache map
			printf( "Found buffer of file - %s in file cache\n", (*fileIter).c_str() );
			WriteCacheBuffer* buffer = writeCache_[ *fileIter ];
			assert( buffer );
			if ( (buffer->status & PINNED) == 0 ) {
				printf( "Buffer for file %s is unpinned. Hence will remove it" 
						 " from the free list and then will pin it back\n", (*fileIter).c_str() );
				// Buffer status not pinned
				pthread_mutex_lock( &freeListMut_ );
				// Remove the buffer from freeList_
				freeList_pop( buffer );
				pthread_mutex_unlock( &freeListMut_ );
				buffer->status = buffer->status | PINNED;
			}
			buffer->pinCount++;			
		} else {
			printf( "Buffer of file - %s does not exist in file cache\n", (*fileIter).c_str() );
			// Buffer does not exist in cache
			if ( writeCache_.size() < max_cache_entries_ ) {
				printf( "File cache limit still not exceeded hence will add new buffer"
						" in file cache for file - %s\n", (*fileIter).c_str() );
				// Create new cache entry
				WriteCacheBuffer *buffer = new WriteCacheBuffer( *fileIter );
				buffer->status = buffer->status | PINNED;
				buffer->pinCount++;
				writeCache_[ *fileIter ] = buffer;		
			} else {
				// Eviction algorithm
				printf( "File cache limit is hit. Can not add new buffer. Running"
						" Eviction Algo for file - %s\n", (*fileIter).c_str() );
				pthread_mutex_lock( &freeListMut_ );
				WriteCacheBuffer *buffer = NULL;
				bool bufFound = false;
				while( !bufFound ) {
					while ( !(freeListSize_ > 0) ) {
						pthread_cond_wait( &freeListCV_ , &freeListMut_ );
						printf( "The wait for free buffer list to get populated is over!\n" );
					}
					buffer = freeList_pop_front();
					assert( !(buffer->status & PINNED) );
					if ( buffer->status & DIRTY ) {
						// Write the buffer first to the disk
						buffer->writeBack();
					}
					bufFound = true;
				}
				pthread_mutex_unlock( &freeListMut_ );
				buffer->reinitialize( *fileIter );
				buffer->status = buffer->status | PINNED;
				buffer->pinCount++;
				assert( buffer );
				writeCache_[ *fileIter ] = buffer;
			}
		}
	}
	pthread_mutex_unlock( &mapMut_ );	
}

void
FileCacheImpl::UnpinFiles( const std::vector<std::string>& file_vec ) {
	// Lock the file cache map
	// Iterate over file_vec
	// If file cache map has buffer for filename
	// Udpate the buffer status, buffer pinCount
	// Lock the list
	// Add the buffer to the list if pinCount drops to zero,
	// 	wakeup threads waiting for free list to get populated
	
	pthread_mutex_lock( &mapMut_ );
	std::vector<std::string>::const_iterator fileIter;
	for ( fileIter = file_vec.begin();
		  fileIter != file_vec.end();
		  ++fileIter ) {
		std::map<std::string, WriteCacheBuffer *>::iterator mapIter;

		if ( (mapIter = writeCache_.find( *fileIter )) != writeCache_.end() ) {
			// File cache map has the buffer for this file
			WriteCacheBuffer *buffer = writeCache_[ *fileIter ];
			assert( buffer );
			if ( buffer->status & PINNED ) {
				assert( buffer->pinCount > 0 );
				buffer->pinCount--;
				if ( buffer->pinCount == 0 ) {
					pthread_mutex_lock( &freeListMut_ );
					buffer->status = buffer->status & ~PINNED;
					freeList_push_back( buffer );
					pthread_cond_broadcast( &freeListCV_ );
					pthread_mutex_unlock( &freeListMut_ );
				}		
				printf( "File to be unpinned is - %s, new status of buffer is - %x\n",
						(*fileIter).c_str(), buffer->status );
			}
		}
	}
	pthread_mutex_unlock( &mapMut_ );
}


const char *
FileCacheImpl::FileData( const std::string& file_name ) {
	
	pthread_mutex_lock( &mapMut_ );

	std::map<std::string, WriteCacheBuffer *>::iterator mapIter;
	if ( (mapIter = writeCache_.find( file_name )) != writeCache_.end() ) {
		// File cache map has buffer for the file_name
		WriteCacheBuffer *buffer = mapIter->second;
		assert( buffer );
		if ( buffer->status & PINNED ) {
			pthread_mutex_unlock( &mapMut_ );
			return (const char *)buffer->buffer;
		}
	}
	pthread_mutex_unlock( &mapMut_ );
	// Returns NULL if buffer is unpinned or file not found in the cache
	return NULL;
}

char *
FileCacheImpl::MutableFileData( const std::string& file_name ) {
	
	pthread_mutex_lock( &mapMut_ );

	std::map<std::string, WriteCacheBuffer *>::iterator mapIter;
	if ( (mapIter = writeCache_.find( file_name )) != writeCache_.end() ) {
		// File cache map has buffer for the file_name
		WriteCacheBuffer *buffer = mapIter->second;
		assert( buffer );
		if ( buffer->status & PINNED ) {
			// Mark the buffer 'dirty'
			buffer->status = buffer->status | DIRTY;
			pthread_mutex_unlock( &mapMut_ );
			return buffer->buffer;
		}
	}
	pthread_mutex_unlock( &mapMut_ );
	// Returns NULL if buffer is unpinned or file not found in the cache
	return NULL;

}

// The factory method used by the application to instantiate file cache
FileCache *
fileCacheFactory( unsigned int max_entries ) {
	FileCache *fc = new FileCacheImpl( max_entries );
	return fc;
}
