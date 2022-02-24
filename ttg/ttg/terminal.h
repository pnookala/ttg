#ifndef TTG_TERMINALS_H
#define TTG_TERMINALS_H

#include <exception>
#include <type_traits>
#include <stdexcept>
#include <any>

#include "ttg/fwd.h"
#include "ttg/base/terminal.h"
#include "ttg/util/meta.h"
#include "ttg/util/trace.h"
#include "ttg/util/demangle.h"
#include "ttg/world.h"
#include "ttg/base/keymap.h"
#include "boost/callable_traits.hpp"

namespace ttg {
  namespace detail {

    /* Wraps any key,value data structure.
     * Elements of the data structure can be accessed using get method, which calls the at method of the Container.
     * tkeyT - taskID
     * keyT - Key type of the Container
     * valueT - Value type of the Container
    */
    template<typename tkeyT, typename keyT, typename valueT>
    struct ContainerWrapper {
      std::function<valueT (keyT const& key)> get = nullptr;
      ttg::meta::detail::keymap_t<keyT> keymap;
      meta::detail::mapper_function_t<tkeyT> mapper;

      ContainerWrapper() = default;

      template<typename T, std::enable_if_t<!std::is_same<std::decay_t<T>,
                                                          ContainerWrapper>{}, bool> = true>
        //Store a pointer to the user's container in std::any, no copies
        ContainerWrapper(T &t) : get([&t](keyT const &key) {
                                if constexpr (!std::is_class_v<T> && std::is_function_v<T>)
                                  return (t)(key);
                                else
                                {
                                  using key_type = typename T::key_type;
                                  //at method returns a const ref to the item.
                                  return t.at(std::any_cast<key_type>(key));
                                }
                              })
        {}
    };

    template <typename valueT> struct ContainerWrapper<void, void, valueT> {
      std::function<std::nullptr_t ()> get = nullptr;
    };

    template <typename keyT> struct ContainerWrapper<void, keyT, void> {
      std::function<std::nullptr_t (keyT const& key)> get = nullptr;
    };

    template <typename valueT> struct ContainerWrapper<ttg::Void, void, valueT> {
      std::function<std::nullptr_t ()> get = nullptr;
    };

    template <> struct ContainerWrapper<void, void, void> {
      std::function<std::nullptr_t ()> get = nullptr;
    };
  } //namespace detail

  template <typename keyT = void, typename valueT = void>
  class In : public TerminalBase {
   public:
    typedef valueT value_type;
    typedef keyT key_type;
    static_assert(std::is_same_v<keyT, std::decay_t<keyT>>,
                  "In<keyT,valueT> assumes keyT is a non-decayable type");
    // valueT can be T or const T
    static_assert(std::is_same_v<std::remove_const_t<valueT>, std::decay_t<valueT>>,
                  "In<keyT,valueT> assumes std::remove_const<T> is a non-decayable type");
    using edge_type = Edge<keyT, valueT>;
    using send_callback_type = meta::detail::send_callback_t<keyT, std::decay_t<valueT>>;
    using move_callback_type = meta::detail::move_callback_t<keyT, std::decay_t<valueT>>;
    using broadcast_callback_type = meta::detail::broadcast_callback_t<keyT, std::decay_t<valueT>>;
    using setsize_callback_type = meta::detail::setsize_callback_t<keyT>;
    using finalize_callback_type = meta::detail::finalize_callback_t<keyT>;
    static constexpr bool is_an_input_terminal = true;

    using mapper_ret_type = boost::callable_traits::return_type_t<meta::detail::mapper_function_t<keyT>>;
    ttg::detail::ContainerWrapper<keyT, mapper_ret_type, valueT> container;
    meta::detail::mapper_function_t<keyT> mapper;

   private:
    send_callback_type send_callback;
    move_callback_type move_callback;
    broadcast_callback_type broadcast_callback;
    setsize_callback_type setsize_callback;
    finalize_callback_type finalize_callback;

    // No moving, copying, assigning permitted
    In(In &&other) = delete;
    In(const In &other) = delete;
    In &operator=(const In &other) = delete;
    In &operator=(const In &&other) = delete;

    void connect(TerminalBase *p) override {
      throw "Edge: to connect terminals use out->connect(in) rather than in->connect(out)";
    }

   public:
    In()
    : TerminalBase(std::is_const_v<valueT> ? TerminalBase::Type::Read : TerminalBase::Type::Consume)
    {}

    void set_callback(const send_callback_type &send_callback, const move_callback_type &move_callback,
                      const broadcast_callback_type &bcast_callback = broadcast_callback_type{},
                      const setsize_callback_type &setsize_callback = setsize_callback_type{},
                      const finalize_callback_type &finalize_callback = finalize_callback_type{}) {
      this->send_callback = send_callback;
      this->move_callback = move_callback;
      this->broadcast_callback = bcast_callback;
      this->setsize_callback = setsize_callback;
      this->finalize_callback = finalize_callback;
    }

    template <typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value>,void>
    send(const Key &key, const Value &value) {
      if (!send_callback) throw std::runtime_error("send callback not initialized");
      send_callback(key, value);
    }

    template <typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value> && std::is_same_v<Value,std::remove_reference_t<Value>>,void>
    send(const Key &key, Value &&value) {
      if (!move_callback) throw std::runtime_error("move callback not initialized");
      move_callback(key, std::forward<valueT>(value));
    }

    template <typename Key = keyT>
    std::enable_if_t<!meta::is_void_v<Key>,void>
    sendk(const Key &key) {
      if (!send_callback) throw std::runtime_error("send callback not initialized");
      send_callback(key);
    }

    template <typename Value = valueT>
    std::enable_if_t<!meta::is_void_v<Value>,void>
    sendv(const Value &value) {
      if (!send_callback) throw std::runtime_error("send callback not initialized");
      send_callback(value);
    }

    template <typename Value = valueT>
    std::enable_if_t<!meta::is_void_v<Value> && std::is_same_v<Value,std::remove_reference_t<Value>>,void>
    sendv(Value &&value) {
      if (!move_callback) throw std::runtime_error("move callback not initialized");
      move_callback(std::forward<valueT>(value));
    }

    void send() {
      if (!send_callback) throw std::runtime_error("send callback not initialized");
      send_callback();
    }

    // An optimized implementation will need a separate callback for broadcast
    // with a specific value for rangeT
    template <typename rangeT, typename Value = valueT>
    std::enable_if_t<!meta::is_void_v<Value>,void>
    broadcast(const rangeT &keylist, const Value &value) {
      if (broadcast_callback) {
        if constexpr (ttg::meta::is_iterable_v<rangeT>) {
          broadcast_callback(ttg::span(&(*std::begin(keylist)), std::distance(std::begin(keylist), std::end(keylist))), value);
        } else {
          /* got something we cannot iterate over (single element?) so put one element in the span */
          broadcast_callback(ttg::span<const keyT>(&keylist, 1), value);
        }
      } else {
        if constexpr (ttg::meta::is_iterable_v<rangeT>) {
          for (auto&& key : keylist) send(key, value);
        } else {
          /* single element */
          send(keylist, value);
        }
      }
    }

    template <typename rangeT, typename Value = valueT>
    std::enable_if_t<!meta::is_void_v<Value>,void>
    broadcast(const rangeT &keylist, Value &&value) {
      const Value& v = value;
      if (broadcast_callback) {
        if constexpr (ttg::meta::is_iterable_v<rangeT>) {
          broadcast_callback(ttg::span<const keyT>(&(*std::begin(keylist)), std::distance(std::begin(keylist), std::end(keylist))), v);
        } else {
          /* got something we cannot iterate over (single element?) so put one element in the span */
          broadcast_callback(ttg::span<const keyT>(&keylist, 1), v);
        }
      } else {
        if constexpr (ttg::meta::is_iterable_v<rangeT>) {
          for (auto&& key : keylist) send(key, v);
        } else {
          /* got something we cannot iterate over (single element?) so put one element in the span */
          broadcast_callback(ttg::span<const keyT>(&keylist, 1), v);
        }
      }
    }

    template <typename Key = keyT>
    std::enable_if_t<!meta::is_void_v<Key>,void>
    set_size(const Key &key, std::size_t size) {
      if (!setsize_callback) throw std::runtime_error("set_size callback not initialized");
      setsize_callback(key, size);
    }

    template <typename Key = keyT>
    std::enable_if_t<meta::is_void_v<Key>,void>
    set_size(std::size_t size) {
      if (!setsize_callback) throw std::runtime_error("set_size callback not initialized");
      setsize_callback(size);
    }

    template <typename Key = keyT>
    std::enable_if_t<!meta::is_void_v<Key>,void>
    finalize(const Key &key) {
      // std::cout << "In::finalize::\n";
      if (!finalize_callback) throw std::runtime_error("finalize callback not initialized");
      finalize_callback(key);
    }

    template <typename Key = keyT>
    std::enable_if_t<meta::is_void_v<Key>,void>
    finalize() {
      if (!finalize_callback) throw std::runtime_error("finalize callback not initialized");
      finalize_callback();
    }
  };

  // Output terminal
  template <typename keyT = void, typename valueT = void>
  class Out : public TerminalBase {
   public:
    typedef valueT value_type;
    typedef keyT key_type;
    static_assert(std::is_same_v<keyT, std::decay_t<keyT>>,
                  "Out<keyT,valueT> assumes keyT is a non-decayable type");
    static_assert(std::is_same_v<valueT, std::decay_t<valueT>>,
                  "Out<keyT,valueT> assumes valueT is a non-decayable type");
    typedef Edge<keyT, valueT> edge_type;
    static constexpr bool is_an_output_terminal = true;

   private:
    // No moving, copying, assigning permitted
    Out(Out &&other) = delete;
    Out(const Out &other) = delete;
    Out &operator=(const Out &other) = delete;
    Out &operator=(const Out &&other) = delete;

   public:
    Out() : TerminalBase(TerminalBase::Type::Write) {}

    /// \note will check data types unless macro \c NDEBUG is defined
    void connect(TerminalBase *in) override {
#ifndef NDEBUG
      if (in->get_type() == TerminalBase::Type::Read) {
        typedef In<keyT, std::add_const_t<valueT>> input_terminal_type;
        if (!dynamic_cast<input_terminal_type *>(in))
          throw std::invalid_argument(
              std::string("you are trying to connect terminals with incompatible types:\ntype of this Terminal = ") +
              detail::demangled_type_name(this) + "\ntype of other Terminal" + detail::demangled_type_name(in));
      } else if (in->get_type() == TerminalBase::Type::Consume) {
        typedef In<keyT, valueT> input_terminal_type;
        if (!dynamic_cast<input_terminal_type *>(in))
          throw std::invalid_argument(
              std::string("you are trying to connect terminals with incompatible types:\ntype of this Terminal = ") +
              detail::demangled_type_name(this) + "\ntype of other Terminal" + detail::demangled_type_name(in));
      } else  // successor->type() == TerminalBase::Type::Write
        throw std::invalid_argument(std::string("you are trying to connect an Out terminal to another Out terminal"));
      trace(rank(), ": connected Out<> ", get_name(), "(ptr=", this, ") to In<> ", in->get_name(), "(ptr=", in, ")");
#endif
      this->connect_base(in);
      //If I am a pull terminal, add me as (in)'s predecessor
      if (is_pull_terminal)
        in->connect_pull(this);
    }

    auto nsuccessors() const {
      return get_connections().size();
    }
    const auto& successors() const {
      return get_connections();
    }

    template <typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value> && std::is_same_v<Value,std::remove_reference_t<Value>>,void>
    send_to(const Key &key, Value &&value, std::size_t i)
    {
      //std::cout << "send_to called for successor " << i << " " << get_name() << "\n";
      TerminalBase *successor = successors().at(i);
      if (successor->get_type() == TerminalBase::Type::Read) {
        static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->send(key, value);
      } else if (successor->get_type() == TerminalBase::Type::Consume) {
        static_cast<In<keyT, valueT> *>(successor)->send(key, value);
      }
    }

    template<typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value>,void> send(const Key &key, const Value &value) {
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->send(key, value);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->send(key, value);
        }
      }
    }

    template<typename Key = keyT, typename Value = valueT>
    std::enable_if_t<!meta::is_void_v<Key> && meta::is_void_v<Value>,void> sendk(const Key &key) {
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->sendk(key);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->sendk(key);
        }
      }
    }

    template<typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_void_v<Key> && !meta::is_void_v<Value>,void> sendv(const Value &value) {
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->sendv(value);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->sendv(value);
        }
      }
    }

    template<typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_all_void_v<Key,Value>,void> send() {
      trace(rank(), ": in ", get_name(), "(ptr=", this, ") Out<>::send: #successors=", successors().size());
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->send();
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->send();
        }
        else {
          throw std::logic_error("Out<>: invalid successor type");
        }
        trace("Out<> ", get_name(), "(ptr=", this, ") send to In<> ", successor->get_name(), "(ptr=", successor, ")");
      }
    }

    template <typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value> && std::is_same_v<Value,std::remove_reference_t<Value>>,void>
    send(const Key &key, Value &&value) {
      const std::size_t N = nsuccessors();
      TerminalBase *move_successor = nullptr;
      // send copies to every terminal except the one we will move the results to
      for (std::size_t i = 0; i != N; ++i) {
        TerminalBase *successor = successors().at(i);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->send(key, value);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          if (nullptr == move_successor) {
            move_successor = successor;
          } else {
            static_cast<In<keyT, valueT> *>(successor)->send(key, value);
          }
        }
      }
      if (nullptr != move_successor) {
        static_cast<In<keyT, valueT> *>(move_successor)->send(key, std::forward<Value>(value));
      }
    }

    // An optimized implementation will need a separate callback for broadcast
    // with a specific value for rangeT
    template<typename rangeT, typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value>,void>
    broadcast(const rangeT &keylist, const Value &value) {  // NO MOVE YET
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->broadcast(keylist, value);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->broadcast(keylist, value);
        }
      }
    }

    template<typename Key = keyT>
    std::enable_if_t<!meta::is_void_v<Key>,void>
    set_size(const Key &key, std::size_t size) {
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->set_size(key, size);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->set_size(key, size);
        }
      }
    }

    template<typename Key = keyT>
    std::enable_if_t<meta::is_void_v<Key>,void>
    set_size(std::size_t size) {
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->set_size(size);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->set_size(size);
        }
      }
    }

    template<typename Key = keyT>
    std::enable_if_t<!meta::is_void_v<Key>,void>
    finalize(const Key &key) {
      for (auto && successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->finalize(key);
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->finalize(key);
        }
      }
    }

    template<typename Key = keyT>
    std::enable_if_t<meta::is_void_v<Key>,void>
    finalize() {
      for (auto successor : successors()) {
        assert(successor->get_type() != TerminalBase::Type::Write);
        if (successor->get_type() == TerminalBase::Type::Read) {
          static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->finalize();
        } else if (successor->get_type() == TerminalBase::Type::Consume) {
          static_cast<In<keyT, valueT> *>(successor)->finalize();
        }
      }
    }
  };

} // namespace ttg

#endif // TTG_TERMINALS_H
