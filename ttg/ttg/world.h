#ifndef TTG_WORLD_H
#define TTG_WORLD_H

#include "ttg/impl_selector.h"

#include <stdexcept>
#include <algorithm>

#include "ttg/base/world.h"
#include "ttg/base/keymap.h"

#include "ttg/fwd.h"

namespace ttg {

  /* Slim wrapper to allow for forward declaration */
  class World : public ttg::base::World<TTG_IMPL_NS::WorldImpl> {
    using ttg::base::World<TTG_IMPL_NS::WorldImpl>::World;
  };

  namespace detail {
      template<typename WorldT>
      WorldT& default_world_accessor() {
        static WorldT world;
        return world;
      }

      template<typename WorldT>
      inline void set_default_world(WorldT&  world) { detail::default_world_accessor<WorldT>() = world; }
      template<typename WorldT>
      inline void set_default_world(WorldT&& world) { detail::default_world_accessor<WorldT>() = std::move(world); }

      template <typename keyT>
      struct default_keymap : ttg::detail::default_keymap_impl<keyT> {
       public:
        default_keymap() = default;
        default_keymap(const ttg::World& world) : ttg::detail::default_keymap_impl<keyT>(world.size()) {}
      };

      template <typename keyT>
      struct default_priomap : ttg::detail::default_priomap_impl<keyT> {
       public:
        default_priomap() = default;
      };

      template<typename WorldImplT>
      std::list<WorldImplT*>&
      world_registry_accessor() {
          static std::list<WorldImplT*> world_registry;
          return world_registry;
      }

      /* TODO: how should the MADNESS and PaRSEC init/finalize play together? */
      template<typename WorldImplT>
      void register_world(WorldImplT& world)
      {
        world_registry_accessor<WorldImplT>().push_back(&world);
      }

      template<typename WorldImplT>
      void deregister_world(WorldImplT& world) {
        auto& world_registry = world_registry_accessor<WorldImplT>();
        auto it = std::find(world_registry.begin(), world_registry.end(), &world);
        if (it != world_registry.end()) {
            world_registry.remove(&world);
        }
      }

      template<typename WorldImplT>
      void destroy_worlds(void) {
        auto& world_registry = world_registry_accessor<WorldImplT>();
        while (!world_registry.empty()) {
            auto it = world_registry.begin();
            (*it)->destroy();
        }
      }

  } // namespace detail

  inline ttg::World &get_default_world() {
    if (detail::default_world_accessor<ttg::World>().is_valid()) {
      return detail::default_world_accessor<ttg::World>();
    } else {
      throw std::runtime_error("ttg::set_default_world() must be called before use");
    }
  }

  inline int rank() {
    int me = -1;

    try {
      me = get_default_world().rank();
    } catch(std::runtime_error&)
    {
      // no default world set, leave -1
    }

#if 0
#if __has_include(<mpi.h>)
    int inited;
    MPI_Initialized(&inited);
    if (inited) {
      int fini;
      MPI_Finalized(&fini);
      if (!fini) {
        auto errcod = MPI_Comm_rank(MPI_COMM_WORLD, &me);
        assert(errcod == 0);
      }
    }
#endif
#endif
    return me;
  }

} // namespace ttg

#endif // TTG_WORLD_H
