package poke.server.resources;

import java.util.List;

import poke.server.storage.jdbc.DatabaseStorage;

import eye.Comm.Request;
import eye.Comm.Response;

/**
 * an abstraction of the server's functionality (processes requests).
 * Implementations of this class will allow the server to handle a variety of
 * requests.
 * This resource interface will mainly be used for chunking
 * 
 */

public interface ChunkedResource {
	
	List<Response> process(Request request, DatabaseStorage dbInst);
}
