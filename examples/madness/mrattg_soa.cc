// TTG AND MADNESS RUNTIME STUFF

#include "ttg.h"

// APPLICATION STUFF BELOW
#include <cmath>
#include <array>
#include <mutex>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <functional>
#include <type_traits>
#include <chrono>

#include "../mragl.h"
#include "../mrakey.h"
#include "../mrahash.h"
#include "../mramxm.h"
#include "../mramisc.h"
#include "../mratypes.h"
#include "../mradomain.h"
#include "../mratwoscale.h"
#include "../mrasimpletensor.h"
#include "../mrafunctionsdata.h"
#include "../mrafunctionnode.h"
#include "../mrafunctionfunctor.h"

#include <madness/world/world.h>
//Required for split md interface
#include <ttg/serialization/splitmd_data_descriptor.h>

using namespace mra;

/// Random process map
template <Dimension NDIM>
struct KeyProcMap {
    const size_t size;
    KeyProcMap() : size(ttg::default_execution_context().size()) {}
    std::size_t operator()(const Key<NDIM>& key) const {return key.hash() % size;}
};


// /// A pmap that locates children on odd levels with their even level parents .. needs a litte fixing
// template <typename keyT>
// class LevelPmap : public WorldDCPmapInterface<keyT> {
// private:
//     const int nproc;
// public:
//     LevelPmap() : nproc(0) {};

//     LevelPmap(World& world) : nproc(world.nproc()) {}

//     /// Find the owner of a given key
//     ProcessID owner(const keyT& key) const {
//         Level n = key.level();
//         if (n == 0) return 0;
//         hashT hash;
//         if (n <= 3 || (n&0x1)) hash = key.hash();
//         else hash = key.parent().hash();
//         return hash%nproc;
//     }
// };

template <Dimension NDIM>
class LevelPmapX {
 private:
     const int nproc;
 public:
    LevelPmapX() : nproc(0) {};

    LevelPmapX(size_t nproc) : nproc(nproc) {}

    /// Find the owner of a given key
    HashValue operator()(const Key<3>& key) const {
      Level n = key.level();
      if (n == 0) return 0;
        madness::hashT hash;
        if (n <= 3 || (n&0x1)) hash = key.hash();
        else hash = key.parent().hash();
        return hash%nproc;
    }
 };

/// A pmap that spatially decomposes the domain and by default slightly overdcomposes to attempt to load balance
template <Dimension NDIM>
class PartitionPmap {
private:
    const int nproc;
    Level target_level;
public:
    PartitionPmap()
        : nproc(1)
        , target_level(3)
    {};

    // Default is to try to optimize the target_level, but you can specify any value > 0
    PartitionPmap(size_t nproc, const Level target_level=0)
        : nproc(nproc)
    {
        if (target_level > 0) {
            this->target_level = target_level;
        }
        else {
            this->target_level = 1;
            int p = nproc-1;
            while (p) {
                p >>= NDIM;
                this->target_level++;
            }
        }
    }

    /// Find the owner of a given key
    HashValue operator()(const Key<NDIM>& key) const {
        HashValue hash;
        if (key.level() <= target_level) {
            hash = key.hash();
        }
        else {
            hash = key.parent(key.level() - target_level).hash();
        }
        return hash%nproc;
    }
};

/// An empty class used for pure control flows
struct Control {};
std::ostream& operator<<(std::ostream& s, const Control& ctl) {s << "Ctl"; return s;}

template <Dimension NDIM> using doubleEdge = ttg::Edge<Key<NDIM>, double>;
template <Dimension NDIM> using ctlEdge = ttg::Edge<Key<NDIM>, Control>;

template <typename T, size_t K, Dimension NDIM, size_t NFUNC> using rnodesEdge = ttg::Edge<Key<NDIM>, FunctionReconstructedNodes<T,K,NDIM,NFUNC>>;
template <typename T, size_t K, Dimension NDIM, size_t NFUNC> using cnodesEdge = ttg::Edge<Key<NDIM>, FunctionCompressedNodes<T,K,NDIM,NFUNC>>;

template <typename T, size_t K, Dimension NDIM, size_t NFUNC> using rnodesOut = ttg::Out<Key<NDIM>, FunctionReconstructedNodes<T,K,NDIM,NFUNC>>;
template <typename T, size_t K, Dimension NDIM, size_t NFUNC> using cnodesOut = ttg::Out<Key<NDIM>, FunctionCompressedNodes<T,K,NDIM,NFUNC>>;

template <Dimension NDIM> using doubleOut = ttg::Out<Key<NDIM>, double>;
template <Dimension NDIM> using ctlOut = ttg::Out<Key<NDIM>, Control>;

std::mutex printer_guard;
template <typename keyT, typename valueT>
auto make_printer(const ttg::Edge<keyT, valueT>& in, const char* str = "", const bool doprint=true) {
    auto func = [str,doprint](const keyT& key, const valueT& value, std::tuple<>& out) {
        if (doprint) {
            std::lock_guard<std::mutex> obolus(printer_guard);
            std::cout << str << " (" << key << "," << value << ")" << std::endl;
        }
    };
    return ttg::make_tt(func, ttg::edges(in), ttg::edges(), "printer", {"input"});
}

template <Dimension NDIM>
auto make_start(const ctlEdge<NDIM>& ctl) {
    auto func = [](const Key<NDIM>& key, std::tuple<ctlOut<NDIM>>& out) { ttg::send<0>(key, Control(), out); };
    return ttg::make_tt<Key<NDIM>>(func, ttg::edges(), edges(ctl), "start", {}, {"control"});
}


/// Constructs an operator that adaptively projects the provided function into the basis

/// Returns an std::unique_ptr to the object
template <typename functorT, typename T, size_t K, Dimension NDIM, size_t NFUNC>
auto make_project(functorT& f,
                  const T thresh, /// should be scalar value not complex
                  ctlEdge<NDIM>& ctl,
                  rnodesEdge<T,K,NDIM,NFUNC>& result,
                  const std::string& name = "project") {

  auto F = [f, thresh](const Key<NDIM>& key, const Control& junk, std::tuple<ctlOut<NDIM>,
                       rnodesOut<T,K,NDIM,NFUNC>>& out) {
             FunctionReconstructedNodes<T,K,NDIM,NFUNC> nodes(key); // Our eventual result
             auto& coeffs = nodes.coeffs; // Need to clean up OO design

             if (key.level() < initial_level(f)) {
               std::vector<Key<NDIM>> bcast_keys;
               /* TODO: children() returns an iteratable object but broadcast()
                  expects a contiguous memory range.
                  We need to fix broadcast to support any ranges */
               for (auto child : children(key)) bcast_keys.push_back(child);
               ttg::broadcast<0>(bcast_keys, Control(), out);
               for (auto i : range(NFUNC))
                 coeffs[i] = T(1e7); // set to obviously bad value to detect incorrect use
               nodes.is_leaf = false;
             }
             else if (is_negligible<functorT,T,NDIM>(f, Domain<NDIM>::
                                                     template bounding_box<T>(key),
                                                     truncate_tol(key,thresh))) {
               for (auto i : range(NFUNC))
                 coeffs[i] = T(0.0);
               nodes.is_leaf = true;
             }
             else {
               for (auto i : range(NFUNC))
                 nodes.is_leaf = fcoeffs<functorT,T,K>(f, key, thresh, coeffs[i]); // cannot deduce K
               if (!nodes.is_leaf) {
                 std::vector<Key<NDIM>> bcast_keys;
                 for (auto child : children(key)) bcast_keys.push_back(child);
                 ttg::broadcast<0>(bcast_keys, Control(), out);
               }
             }
             ttg::send<1>(key, std::move(nodes), out); // always produce a result
           };
    ctlEdge<NDIM> refine("refine");
    return ttg::make_tt(F, edges(fuse(refine, ctl)), ttg::edges(refine, result),
                        name, {"control"}, {"refine", "result"});
}

// Stream leaf nodes up the tree as a prelude to compressing
template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
void send_leaves_up(const Key<NDIM>& key,
                    const FunctionReconstructedNodes<T,K,NDIM,NFUNC>& nodes,
                    std::tuple<rnodesOut<T,K,NDIM,NFUNC>,
                    cnodesOut<T,K,NDIM,NFUNC>>& out) {
    nodes.sum = 0.0;   //
    if (!nodes.has_children()) { // We are only interested in the leaves
        if (key.level() == 0) {  // Tree is just one node
            throw "not yet";
        } else {
          ttg::send<0>(key.parent(), nodes, out);
        }
    }
}

template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
void reduce_leaves(const Key<NDIM>& key,
                   const FunctionReconstructedNodes<T,K,NDIM,NFUNC>& nodes,
                   std::tuple<rnodesOut<T,K,NDIM,NFUNC>>& out) {
  std::get<0>(out).send(key, nodes);
}

// With data streaming up the tree run compression
template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
void do_compress(const Key<NDIM>& key,
                 const FunctionReconstructedNodes<T,K,NDIM,NFUNC> &in,
                 std::tuple<rnodesOut<T,K,NDIM,NFUNC>, cnodesOut<T,K,NDIM,NFUNC>> &out) {
    auto& child_slices = FunctionData<T,K,NDIM>::get_child_slices();
    FunctionCompressedNodes<T,K,NDIM,NFUNC> result(key); // The eventual result
    auto& d = result.coeffs;

    std::array<T,NFUNC> sumsq = { 0.0 };
    for (auto j : range(NFUNC)) {
      // Collect child coeffs and leaf info
      FixedTensor<T,2*K,NDIM> s;
      for (size_t i : range(Key<NDIM>::num_children)) {
        s(child_slices[i]) = in.neighbor_coeffs[j][i];
        result.is_leaf[i] = in.is_neighbor_leaf[j][i];
        sumsq[j] += in.neighbor_sum[j][i]; // Accumulate sumsq from child difference coeffs
      }
      filter<T,K,NDIM>(s,d[j]);  // Apply twoscale transformation
    }

    // Recur up
    if (key.level() > 0) {
      FunctionReconstructedNodes<T,K,NDIM,NFUNC> p(key);
      for (auto i : range(NFUNC)) {
        p.coeffs[i] = d[i](child_slices[0]);
        d[i](child_slices[0]) = 0.0;
        // Accumulate sumsq of difference coeffs from this node and children
        p.sum = d[i].sumabssq() + sumsq[i];
      }
      ttg::send<0>(key.parent(), std::move(p), out);
    }
    else {
      for (auto i : range(NFUNC))
        std::cout << "At root of compressed tree: total normsq is " <<
          sumsq[i] + d[i].sumabssq() << std::endl;
    }

    // Send result to output tree
    ttg::send<1>(key, std::move(result), out);
}


/// Return a string with the binary encoding of the lowest \c width bits of the given integer \c i
std::string int2bitstring(size_t i, size_t width) {
    std::string s="";
    for (auto d : range(width)) {
        s = (((i>>d)&0x1) ? "1" : "0") + s;
        //i>>=1;
    }
    return s;
}

/// Make a composite operator that implements compression for a single function
template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
auto make_compress(rnodesEdge<T,K,NDIM,NFUNC>& in, cnodesEdge<T,K,NDIM,NFUNC>& out,
                   const std::string& name = "compress") {
  rnodesEdge<T,K,NDIM,NFUNC> children1("children1"), children2("children2");

  return std::make_tuple(ttg::make_tt(&send_leaves_up<T,K,NDIM,NFUNC>, edges(in),
                                      edges(children1, out),
                                      "send_leaves_up", {"input"}, {"children1", "output"}),
                         ttg::make_tt(&reduce_leaves<T,K,NDIM,NFUNC>, edges(children1),
                                      edges(children2),
                                      "reduce_leaves", {"children1"}, {"children2"}),
                         ttg::make_tt(&do_compress<T,K,NDIM,NFUNC>, edges(children2),
                                      edges(children1,out),
                                      "do_compress", {"children2"}, {"recur","output"}));
}

template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
void do_reconstruct(const Key<NDIM>& key,
                    const std::tuple<FunctionCompressedNodes<T,K,NDIM,NFUNC>&,
                    std::array<FixedTensor<T,K,NDIM>,NFUNC>&>& t,
                    std::tuple<ttg::Out<Key<NDIM>,std::array<FixedTensor<T,K,NDIM>,NFUNC>>,
                    rnodesOut<T,K,NDIM,NFUNC>>& out) {
  const auto& child_slices = FunctionData<T,K,NDIM>::get_child_slices();
    auto& nodes = std::get<0>(t);
    const auto& from_parent = std::get<1>(t);
    std::array<FixedTensor<T,2*K,NDIM>,NFUNC> s;
    for (auto i : range(NFUNC)) {
      if (key.level() != 0) nodes.coeffs[i](child_slices[0]) = from_parent[i];

      unfilter<T,K,NDIM>(nodes.coeffs[i], s[i]);
    }

    std::array<std::vector<Key<NDIM>>, 2> bcast_keys;

    FunctionReconstructedNodes<T,K,NDIM,NFUNC> r(key);
    for (auto i : range(NFUNC)) r.coeffs[i] = T(0.0);
    r.is_leaf = false;
    //::send<1>(key, r, out); // Send empty interior node to result tree
    bcast_keys[1].push_back(key);

    KeyChildren<NDIM> children(key);
    for (auto it=children.begin(); it!=children.end(); ++it) {
      const Key<NDIM> child= *it;
      r.key = child;
      for (auto i : range(NFUNC)) {
        r.coeffs[i] = s[i](child_slices[it.index()]);
      }
      r.is_leaf = nodes.is_leaf[it.index()];
      if (r.is_leaf) {
        //::send<1>(child, r, out);
        bcast_keys[1].push_back(child);
      }
      else {
        //::send<0>(child, r.coeffs, out);
        bcast_keys[0].push_back(child);
      }
    }
    ttg::broadcast<0>(bcast_keys[0], r.coeffs, out);
    ttg::broadcast<1>(bcast_keys[1], std::move(r), out);
}

template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
auto make_reconstruct(const cnodesEdge<T,K,NDIM,NFUNC>& in,
                      rnodesEdge<T,K,NDIM,NFUNC>& out,
                      const std::string& name = "reconstruct") {
  ttg::Edge<Key<NDIM>,std::array<FixedTensor<T,K,NDIM>,NFUNC>> S("S");
  // passes scaling functions down

  auto s = ttg::make_tt_tpl(&do_reconstruct<T,K,NDIM,NFUNC>, ttg::edges(in, S),
                            ttg::edges(S, out), name, {"input", "s"}, {"s", "output"});

    if (ttg::default_execution_context().rank() == 0) {
      s->template in<1>()->send(Key<NDIM>{0,{0}},
                                std::array<FixedTensor<T,K,NDIM>,NFUNC>{ FixedTensor<T,K,NDIM>() });
      // Prime the flow of scaling functions
    }

    return s;
}

template <typename keyT, typename valueT>
auto make_sink(const ttg::Edge<keyT,valueT>& e) {
    return std::make_unique<ttg::SinkTT<keyT,valueT>>(e);
}

// For checking we haven't broken something while developing
template <typename T>
struct is_serializable {
    static const bool value = std::is_fundamental<T>::value || std::is_member_function_pointer<T>::value || std::is_function<T>::value  || std::is_function<typename std::remove_pointer<T>::type>::value || std::is_pod<T>::value;
};
static_assert(is_serializable<Key<2>>::value, "You just did something that stopped Key from being serializable"); // yes
static_assert(is_serializable<SimpleTensor<float,2,2>>::value,"You just did something that stopped SimpleTensor from being serializable"); // yes
/* this does not hold anymore */
//static_assert(is_serializable<FunctionReconstructedNode<float,2,2>>::value,"You just did something that stopped FunctionReconstructedNode from being serializable"); // yes

// Test gaussian function
template <typename T, Dimension NDIM>
T g(const Coordinate<T,NDIM>& r) {
    static const T expnt = 3.0;
    static const T fac = std::pow(T(2.0*expnt/M_PI),T(0.25*NDIM)); // makes square norm over all space unity
    T rsq = 0.0;
    for (auto x : r) rsq += x*x;
    return fac*std::exp(-expnt*rsq);
}

// Test gaussian functor
template <typename T, Dimension NDIM>
class Gaussian {
    const T expnt;
    const Coordinate<T,NDIM> origin;
    const T fac;
    const T maxr;
    Level initlev;
public:
    Gaussian(T expnt, const Coordinate<T,NDIM>& origin)
        : expnt(expnt)
        , origin(origin)
        , fac(std::pow(T(2.0*expnt/M_PI),T(0.25*NDIM)))
        , maxr(std::sqrt(std::log(fac/1e-12)/expnt))
    {
        // Pick initial level such that average gap between quadrature points
        // will find a significant value
        const int N = 6; // looking for where exp(-a*x^2) < 10**-N
        const int K = 6; // typically the lowest order of the polyn
        const T log10 = std::log(10.0);
        const T log2 = std::log(2.0);
        const T L = Domain<NDIM>::get_max_width();
        const T a = expnt*L*L;
        double n = std::log(a/(4*K*K*(N*log10+std::log(fac))))/(2*log2);
        //std::cout << expnt << " " << a << " " << n << std::endl;
        initlev = Level(n<2 ? 2.0 : std::ceil(n));
    }

    // T operator()(const Coordinate<T,NDIM>& r) const {
    //     T rsq = 0.0;
    //     for (auto x : r) rsq += x*x;
    //     return fac*std::exp(-expnt*rsq);
    // }

    template <size_t N>
    void operator()(const SimpleTensor<T,NDIM,N>& x, std::array<T,N>& values) const {
        distancesq(origin, x, values);
	for (T& value : values) {
          value = fac * std::exp(-expnt*value);
        }
    }

    Level initial_level() const {
        return this->initlev;
    }

    bool is_negligible(const std::pair<Coordinate<T,NDIM>,Coordinate<T,NDIM>>& box, T thresh) const {
        auto& lo = box.first;
        auto& hi = box.second;
        T rsq = 0.0;
        T maxw = 0.0; // max width of box
        for (Dimension d : range(NDIM)) {
            maxw = std::max(maxw,hi(d)-lo(d));
            T x = T(0.5)*(hi(d)+lo(d)) - origin(d);
            rsq += x*x;
        }
        static const T diagndim = T(0.5)*std::sqrt(T(NDIM));
        T boxradplusr = maxw*diagndim + maxr;
        // ttg::print(box, boxradplusr, bool(boxradplusr*boxradplusr < rsq));
        return (boxradplusr*boxradplusr < rsq);
    }
};

template <typename T, size_t K, Dimension NDIM, size_t NFUNC>
void test2(T thresh = 1e-6) {
    FunctionData<T,K,NDIM>::initialize();
    //PartitionPmap<NDIM> pmap =  PartitionPmap<NDIM>(ttg::default_execution_context().size());
    Domain<NDIM>::set_cube(-6.0,6.0);
    LevelPmapX<NDIM> pmap = LevelPmapX<NDIM>(ttg::default_execution_context().size());

    srand48(5551212); // for reproducible results
    for (auto i : range(10000)) drand48(); // warmup generator

    ctlEdge<NDIM> ctl("start");
    auto start = make_start(ctl);
    std::vector<std::unique_ptr<ttg::TTBase>> ops;
    //    for (auto i : range(nfunc)) {
    T expnt = 30000.0;
    Coordinate<T,NDIM> r;
    for (size_t d=0; d<NDIM; d++) {
      r[d] = T(-6.0) + T(12.0)*drand48();
    }
    auto ff = Gaussian<T,NDIM>(expnt, r);

    rnodesEdge<T,K,NDIM,NFUNC> a("a"), c("c");
    cnodesEdge<T,K,NDIM,NFUNC> b("b");

    auto p1 = make_project(ff, T(thresh), ctl, a, "project A");
    p1->set_keymap(pmap);

    auto compress = make_compress<T,K,NDIM,NFUNC>(a, b);
    std::get<0>(compress)->set_keymap(pmap);
    std::get<1>(compress)->set_keymap(pmap);
    std::get<2>(compress)->set_keymap(pmap);

    auto &reduce_leaves_op = std::get<1>(compress);
    reduce_leaves_op->template set_input_reducer<0>
      ([](FunctionReconstructedNodes<T,K,NDIM,NFUNC> &node,
          const FunctionReconstructedNodes<T,K,NDIM,NFUNC> &another)
       {
         for (auto i : range(NFUNC)) {
           //Update self values into the array.
           node.neighbor_coeffs[i][node.key.childindex()] = node.coeffs[i];
           node.is_neighbor_leaf[i][node.key.childindex()] = node.is_leaf;
           node.neighbor_sum[i][node.key.childindex()] = node.sum;
           node.neighbor_coeffs[i][another.key.childindex()] = another.coeffs[i];
           node.is_neighbor_leaf[i][another.key.childindex()] = another.is_leaf;
           node.neighbor_sum[i][another.key.childindex()] = another.sum;
         }
       });
    reduce_leaves_op->template set_static_argstream_size<0>(1 << NDIM);

    auto recon = make_reconstruct<T,K,NDIM,NFUNC>(b,c);
    recon->set_keymap(pmap);

    //auto printer =   make_printer(a,"projected    ", true);
    // auto printer2 =  make_printer(b,"compressed   ", false);
    // auto printer3 =  make_printer(c,"reconstructed", false);
    auto printer =   make_sink(a);
    auto printer2 =  make_sink(b);
    auto printer3 =  make_sink(c);

    ops.push_back(std::move(p1));
    ops.push_back(std::move(std::get<0>(compress)));
    ops.push_back(std::move(std::get<1>(compress)));
    ops.push_back(std::move(std::get<2>(compress)));
    ops.push_back(std::move(recon));
    ops.push_back(std::move(printer));
    ops.push_back(std::move(printer2));
    ops.push_back(std::move(printer3));
    //}

    std::chrono::time_point<std::chrono::high_resolution_clock> beg, end;
    auto connected = make_graph_executable(start.get());
    assert(connected);
    if (ttg::default_execution_context().rank() == 0) {
        //std::cout << "Is everything connected? " << connected << std::endl;
        //std::cout << "==== begin dot ====\n";
        //std::cout << Dot()(start.get()) << std::endl;
        //std::cout << "====  end dot  ====\n";
	beg = std::chrono::high_resolution_clock::now();
        // This kicks off the entire computation
        start->invoke(Key<NDIM>(0, {0}));
    }

    ttg::execute();
    ttg::fence();

    if (ttg::default_execution_context().rank() == 0) {
        end = std::chrono::high_resolution_clock::now();
        std::cout << "TTG Execution Time (seconds) : "
              << (std::chrono::duration_cast<std::chrono::microseconds>(end - beg).count()) / 1000000.0 << std::endl;

    }
}

constexpr const size_t NFUNC = 2;

int main(int argc, char** argv) {
    ttg::initialize(argc, argv, -1);

    //std::cout << "Hello from madttg\n";

    //vmlSetMode(VML_HA | VML_FTZDAZ_OFF | VML_ERRMODE_DEFAULT); // default
    //vmlSetMode(VML_EP | VML_FTZDAZ_OFF | VML_ERRMODE_DEFAULT); // err is 10x default
    //vmlSetMode(VML_HA | VML_FTZDAZ_ON | VML_ERRMODE_DEFAULT); // err is same as default little faster
    //vmlSetMode(VML_EP | VML_FTZDAZ_ON  | VML_ERRMODE_DEFAULT); // err is 10x default

    GLinitialize();

    {
        //test0<float,6,3>();
        //test1<float,6,3>();
        //test2<float,6,3>(20);
      test2<double,10,3,NFUNC>(1e-8);
        //test1<double,6,3>();
    }

    ttg::fence();

    ttg::finalize();


    return 0;
}
