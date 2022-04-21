#ifndef TTG_TERMINALS_H
#define TTG_TERMINALS_H

#include <exception>
#include <type_traits>
#include <stdexcept>

#include "ttg/fwd.h"
#include "ttg/base/terminal.h"
#include "ttg/util/meta.h"
#include "ttg/util/trace.h"
#include "ttg/util/demangle.h"
#include "ttg/world.h"
#include "boost/callable_traits.hpp"

namespace ttg {
  namespace detail {

    /* Wraps any key,value data structure.
     * Elements of the data structure can be accessed using get method, which calls the at method of the Container.
     * keyT - taskID
     * valueT - Value type of the Container
    */
    template<typename keyT, typename valueT>
    struct ContainerWrapper {
      std::function<valueT (keyT const& key)> get = nullptr;
      std::function<size_t (keyT const& key)> owner = nullptr;

      ContainerWrapper() = default;

      template<typename T, typename mapperT, typename keymapT,
               std::enable_if_t<!std::is_same<std::decay_t<T>,
                                              ContainerWrapper>{}, bool> = true>
        //Store a pointer to the user's container, no copies
        ContainerWrapper(T &t, mapperT &&mapper,
                         keymapT &&keymap) : get([&t,mapper = std::forward<mapperT>(mapper)](keyT const &key) {
                                                   if constexpr (!std::is_class_v<T> && std::is_invocable_v<T, keyT>) {
                                                      auto k = mapper(key);
                                                      return t(k); //Call the user-defined lambda function.
                                                    }
                                                  else
                                                    {
                                                      auto k = mapper(key);
                                                      //at method returns a const ref to the item.
                                                      return t.at(k);
                                                    }
                                                }),
                                             //TODO: put([]T::key_type key, valueT &value) {}),
                                            owner([&t, mapper = std::forward<mapperT>(mapper), keymap = std::forward<keymapT>(keymap)](keyT const &key) {
                                                    auto idx = mapper(key); //Mapper to map task ID to index of the data structure.
                                                    return keymap(idx);
                                                  })
        {}
    };

    template <typename valueT> struct ContainerWrapper<void, valueT> {
      std::function<valueT ()> get = nullptr;
      std::function<size_t ()> owner = nullptr;
    };

    template <typename keyT> struct ContainerWrapper<keyT, void> {
      std::function<std::nullptr_t (keyT const& key)> get = nullptr;
      std::function<size_t (keyT const& key)> owner = nullptr;
    };

    template <typename valueT> struct ContainerWrapper<ttg::Void, valueT> {
      std::function<valueT ()> get = nullptr;
      std::function<size_t ()> owner = nullptr;
    };

    template <> struct ContainerWrapper<void, void> {
      std::function<std::nullptr_t ()> get = nullptr;
      std::function<size_t ()> owner = nullptr;
    };

      /* Wraps mapper for pull TTs.
     * keyT - taskID
    */
    template<typename keyT>
    struct TTMapper {
      std::function<size_t (keyT const& key)> owner = nullptr;
      std::function<keyT (keyT const& key)> index = nullptr;
      bool set = false;
      TTMapper() = default;

      template<typename mapperT, typename keymapT, std::enable_if_t<!std::is_same<std::decay_t<mapperT>,
                                                          TTMapper>{}, bool> = true>
        TTMapper(mapperT &&mapper,
                 keymapT &&keymap) : owner([mapper = std::forward<mapperT>(mapper), keymap = std::forward<keymapT>(keymap)](keyT const &key) {
                                             auto idx = mapper(key); //Mapper to map task ID to index of the data structure.
                                             return keymap(idx);
                                           }),
                                     index([mapper = std::forward<mapperT>(mapper)] (keyT const &key) {
                                             return mapper(key);
                                           }),
                                     set(true)
        {}
    };

    template<> struct TTMapper<void> {
      std::function<size_t ()> owner = [](){ return 0; };
    };
  } //namespace detail

  //Forward declaration
  template <typename keyT, typename valueT>
  class Out;

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
    ttg::detail::ContainerWrapper<keyT, valueT> container;
    ttg::detail::TTMapper<keyT> mapper;

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

    template <typename Key, size_t i>
    void invoke_predecessor(const std::tuple<Key, Key> &keys) {
      //TODO: What happens when there are multiple predecessors?
      std::size_t s = 0;
      bool found = false;
      std::cout << "invoke_predecessor called\n";
      for (auto && predecessor : predecessors_) {
        //Find out which successor I am
        /*for (auto&& successor : this->successors_) {
          if (successor != this)
            s++;
          else {
            found = true;
            break;
          }
        }
        if (found) {
        std::cout << "Found the successor, invoking callback\n";*/
          static_cast<Out<Key, void>*>(predecessor)->invoke_pulltask_callback(keys, i);
          /*}
            else throw std::runtime_error("Pull TT successor not found!");*/
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
    using pulltask_callback_type = meta::detail::pulltask_callback_t<keyT>;

   private:
    pulltask_callback_type pulltask_callback;
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

    void set_pulltask_callback(const pulltask_callback_type &pulltask_callback) {
      this->pulltask_callback = pulltask_callback;
    }

    template <typename Key = keyT>
    std::enable_if_t<!meta::is_void_v<Key>, void>
    invoke_pulltask_callback(const Key& key, const std::size_t s) {
      if (!pulltask_callback)
        throw std::runtime_error("pull task callback not initialized");
      pulltask_callback(key, s);
    }

    auto nsuccessors() const {
      return get_connections().size();
    }
    const auto& successors() const {
      return get_connections();
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

    template <typename Key = keyT, typename Value = valueT>
    std::enable_if_t<meta::is_none_void_v<Key,Value> && std::is_same_v<Value,std::remove_reference_t<Value>>,void>
    send_to(const Key &key, Value &&value, std::size_t i)
    {
      TerminalBase *successor = successors().at(i);
      if (successor->get_type() == TerminalBase::Type::Read) {
        static_cast<In<keyT, std::add_const_t<valueT>> *>(successor)->send(key, value);
      } else if (successor->get_type() == TerminalBase::Type::Consume) {
        static_cast<In<keyT, valueT> *>(successor)->send(key, value);
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
