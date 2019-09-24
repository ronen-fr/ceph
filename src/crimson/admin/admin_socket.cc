


// header

#include "admin_socket.h"

seastar::future<> AdminNbSocket::init(cont std::string path)
{
  ldout(m_cct, 5) << "init " << path << dendl;

  // initialize the shutdown pipe


  // bind & listen to a newly created admin stream
  

}
