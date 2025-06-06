//
// Created by teama on 08-02-2025.
//

#ifndef SERIALIZABLE_H
#define SERIALIZABLE_H


template <typename T, typename C>
class ISerializable
{
    public:
    virtual ~ISerializable() = default;
    virtual void serialize() = 0;
    virtual C deserialize()= 0;
};
#endif //SERIALIZABLE_H
