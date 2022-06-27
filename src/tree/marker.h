#ifndef CALICO_TREE_MARKER_H
#define CALICO_TREE_MARKER_H

#include "calico/common.h"
#include "utils/identifier.h"

namespace calico {

class ITree;

class Marker {
public:
    explicit Marker(ITree*);

private:
    ITree *m_tree;
    Index m_index;
    Size m_size;
    PID m_id;
};

} // calico

#endif // CALICO_TREE_MARKER_H
