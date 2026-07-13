ifdef FD_HAS_HOSTED
ifdef FD_HAS_ALLOCA
$(call add-objs,fd_stream_tile fd_stream_proto fd_stream_grpc,fd_discof)
endif
endif
